
from datetime import datetime, timedelta
import logging
from time import sleep
from random import choice

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator

from helper.models import Article
from helper.kafka_produce import make_producer, ArticleProducerCallback
from helper import schemas
from helper.bs4_functions import get_soup

from tickets.tickets import TICKETS, NASDAQ, NYSE

# Constants for Kafka
TOPIC_NAME = "test.articles_v2"
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# Rotate user_agent to avoid "ban"
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127.0.0.0'
]

# How many seconds to wait after each request, to avoid erorr 429
def wait_and_rotate_agent(wait_time=10):
    sleep(wait_time)
    return {'User-Agent': choice(user_agents)}


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def motley_fool_extract():
    """
    DAG to extract article data from Motley Fool, process the data,
    and produce it to a Kafka topic.
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
            tickets (list): The stock ticker symbols.
        
        Returns:
            list: A list of dictionaries containing article metadata; url, title, ticket.
        """
        article_basic_info = []
        headers = wait_and_rotate_agent(wait_time=0)
        
        # Change the link to scrape based on the exchange the ticket is listed on
        for ticket in tickets.keys():
            if tickets[ticket] == NASDAQ:
                url = f'https://www.fool.com/quote/nasdaq/{ticket}/'
                soup = get_soup(logger=logger, ticket=ticket, urls=[url], headers=headers)
            else:
                url = f'https://www.fool.com/quote/nyse/{ticket}/'
                soup = get_soup(logger=logger, ticket=ticket, urls=[url], headers=headers)
            
            # For logging
            try:
                # Locate the section on the main page with the article links
                news_div = soup.find(id="quote-news-analysis")
                
                # get the title, link and ticket of the first article
                article_basic_info += [{'url': a.get('href'), 'title': a.get('data-track-link'), 'ticket': ticket} for a in news_div.find_all('a')[:1]]
                    
                # There's no check to see if the ticket is in the title because fool.com doesn't show ads in that div.
                        
                logger.info(f"Successfully retrieved article link for ticket: {ticket}.")
            
            except Exception as e:
                logger.error(e)
                logger.error(f'The html sctructure is: {soup}')
                raise
            
            # Wait after each request to avoid error 429
            headers = wait_and_rotate_agent()
                    
        logger.info(f"Total articles found: {len(article_basic_info)}.")
        return article_basic_info


    @task(
        task_id='process_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        trigger_rule='all_done'
    )
    def process_links_task(article_basic_info):
        """
        Task to process raw article data, extract content, and produce it to a Kafka topic.
        
        Args:
            raw_articles (list): List of dictionaries containing raw article metadata; link, title, ticket.
        """
        # Useful to not mark the task as 'upstream_failed' when the one above fails. That's why trigger_rule='all_done'
        if not article_basic_info:
            logger.warning("No articles to process.")
            return
        
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.article_schema_v1
        )
        logger.info("Kafka producer created successfully.")
        
        # Even if an article fails, try-except-finally will produce all the successful messages to Kafka
        soup = 'None'
        errors = 0
        headers = wait_and_rotate_agent(wait_time=0)
        try:
            for basic_article in article_basic_info:
            
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
                            url='https://www.fool.com/' + basic_article['url'],
                            title= basic_article['title'],
                            article_body= article_text,
                            timestp=int(datetime.now().timestamp())
                        )
                        
                        producer.produce(
                            topic=TOPIC_NAME,
                            key=article.ticket,
                            value=article,
                            on_delivery=ArticleProducerCallback(article)
                        )
                    
                    else:
                        logger.warning(f"No text found for the article: {basic_article['url']} -- ticket: {basic_article['ticket']}.")
                
                else:
                    logger.warning(f"No article body found for the ticket: {basic_article['ticket']} -- url: {basic_article['url']}")
                
                # Wait after each request to avoid error 429
                headers = wait_and_rotate_agent()
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
        text='''I couldn't extract the link fron Motley Fool.
        
        The dag that failed is motley_fool_extract. The failed task is get_news_links_task.
        
        Probably, the website changed the html.''',
        trigger_rule='all_failed'
    )
    
    telegram_failure_msg_process = TelegramOperator(
        task_id='telegram_failure_msg_process',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''I couldn't process all the links from Motley Fool.
        
        The dag that failed is motley_fool_extract. The failed task is process_links_task.
        
        Probably, the website changed the html.''',
        trigger_rule='all_failed'
    )

    get_news_links = get_news_links_task(TICKETS)
    process_links = process_links_task(get_news_links)
    
    get_news_links >> [process_links, telegram_failure_msg_extract]
    process_links >> telegram_failure_msg_process

motley_fool_extract()
