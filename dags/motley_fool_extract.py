
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.models import Article
from helper.kafka_produce import make_producer, ArticleProducerCallback
from helper import schemas
from helper.bs4_functions import get_soup, wait_and_rotate_agent
from helper.cached_mongo import get_cached_articles

from tickets.tickets import TICKETS, NASDAQ, NYSE


# Constants for Kafka
TOPIC_NAME = "test.articles_v2"
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")
SOURCE = 'motley_fool'

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dag(
    schedule="0 13 * * 5",  # Every Friday at 13
    start_date=datetime(2024, 10, 5),
    catchup=False,
    tags=["stock_sentiment"]
)
def motley_fool_extract():
    """
    This DAG extracts article data from Motley Fool, checks for duplicates
    using a MongoDB cache, and produces unique articles to a Kafka topic.
    
    It sends Telegram notifications in case of failure.
    """

    @task(
        task_id='get_news_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        depends_on_past=False
    )
    def get_news_links_task(tickets):
        """
        Task to retrieve news links for specific stocks from Motley Fool.
        
        Args:
            tickets (dict): A dictionary where the key is the stock ticker symbol, and
                            the value is the exchange where it's listed (NASDAQ, NYSE).
        
        Returns:
            list: A list of dictionaries containing article metadata (url, title, ticket, duplicate).
        """
        article_basic_info = []
        headers = wait_and_rotate_agent(wait_time=0)
        
        # Change the link to scrape based on the exchange the ticket is listed on
        for ticket in list(tickets.keys())[:2]:
            if tickets[ticket] == NASDAQ:
                url = f'https://www.fool.com/quote/nasdaq/{ticket}/'
            else:
                url = f'https://www.fool.com/quote/nyse/{ticket}/'
            
            # For logging
            try:
                soup = get_soup(logger=logger, ticket=ticket, urls=[url], headers=headers)
                
                # Locate the section on the main page with the article links
                news_div = soup.find(id="quote-news-analysis")
                
                # get the title, link and ticket of the first article, that's why there's [:1] at the end
                article_basic_info += [{'url': a.get('href'),
                                        'title': a.get('data-track-link'),
                                        'ticket': ticket,
                                        'duplicate': False} for a in news_div.find_all('a')[:1]]
                    
                # There's no check to see if the ticket is in the title because fool.com doesn't show ads in that div.
                        
                logger.info(f"Successfully retrieved article link for ticket: {ticket}.")
            
            except Exception as e:
                logger.error(f"Failed to retrieve articles for {ticket}. Exception: {e}")
                logger.error(f'The html sctructure is: {soup}')
                raise
            
            # Wait after each request to avoid error 429
            headers = wait_and_rotate_agent()
                    
        logger.info(f"Total articles found: {len(article_basic_info)}.")
        return article_basic_info


    @task(
        task_id='check_for_duplicates',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def check_for_duplicates_task(article_basic_info):
        """
        Checks if the articles retrieved from the API are already in the cache.

        Args:
            articles_metadata (list): A list of article metadata (url, title, ticket, duplicate).
        
        Returns:
            list: Updated list of article metadata, marking duplicates.
        """
        
        mongo_hook = MongoHook(conn_id='mongo_test')
        client = mongo_hook.get_conn()
        cached_articles = get_cached_articles(client=client, source=SOURCE)

        for article in article_basic_info:
            cached_title = cached_articles.get(article['ticket'].lower())
            if article['title'] == cached_title:
                article['duplicate'] = True

        logger.info(f"Checked {len(article_basic_info)} articles for duplicates.")
        return article_basic_info


    @task(
        task_id='process_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def process_links_task(article_basic_info):
        """
        Process the article metadata, extract content, and produce it to Kafka.
        
        Args:
            article_basic_info (list): List of article metadata (url, title, ticket, duplicate).
        """

        if not article_basic_info:
            logger.warning("No articles to process.")
            return
        
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.article_schema_v2
        )
        logger.info("Kafka producer created successfully.")
        
        # Even if an article fails, try-except-finally will produce all the successful messages to Kafka
        soup = 'None'
        errors = 0
        headers = wait_and_rotate_agent(wait_time=0)
        try:
            for basic_article in article_basic_info:
                
                if not basic_article['duplicate']:
                    url = 'https://www.fool.com/' + basic_article['url']
                    soup = get_soup(logger=logger, ticket=basic_article['ticket'], urls=[url], headers=headers)
                    logger.info(f'Request made for: {url}')
                
                    article_body = soup.select('div.article-body')
                    
                    if article_body:
                        article_body = article_body[0]
                        article_text = '\n'.join(paragraph.get_text() for paragraph in article_body.find_all('p'))
                        
                        if article_text:
                            article = Article(
                                ticket=basic_article['ticket'].lower(),
                                url=url,
                                title= basic_article['title'],
                                article_body= article_text,
                                timestp=int(datetime.now().timestamp()),
                                source=SOURCE
                            )
                            
                            producer.produce(
                                topic=TOPIC_NAME,
                                key=article.ticket,
                                value=article,
                                on_delivery=ArticleProducerCallback(article)
                            )
                        
                        else:
                            logger.warning(f"No text found for article at {url}")
                    
                    else:
                        logger.warning(f"No article body found for URL: {url}")
                    
                    # Wait after each request to avoid error 429
                    headers = wait_and_rotate_agent()
                    
                else:
                    logger.info(f'Found a duplicate article for {basic_article["ticket"]} -- {basic_article["title"]}')
                
        except Exception as e:
            logger.error('-------------------------------------------------')
            logger.error(e)
            logger.error(f'The html sctructure is: {soup}')
            logger.error('-------------------------------------------------')
            errors += 1
        
        finally:
            producer.flush()
            logger.info("Kafka producer flushed successfully.")
        
        if errors > 0:
            raise
        
        
    telegram_failure_msg_extract = TelegramOperator(
        task_id='telegram_failure_msg_extract',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''I couldn't extract links from Motley Fool in `motley_fool_extract` DAG.''',
        trigger_rule='all_failed'
    )
    
    telegram_failure_msg_process = TelegramOperator(
        task_id='telegram_failure_msg_process',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''I couldn't process some or all articles in `motley_fool_extract` DAG.''',
        trigger_rule='all_failed'
    )

    get_news_links = get_news_links_task(TICKETS)
    check_for_duplicates = check_for_duplicates_task(get_news_links)
    process_links = process_links_task(check_for_duplicates)
    
    get_news_links >> [check_for_duplicates, telegram_failure_msg_extract]
    check_for_duplicates >> [process_links, telegram_failure_msg_process]
    process_links >> telegram_failure_msg_process

motley_fool_extract()
