
from datetime import datetime, timedelta
import requests
import logging
from bs4 import BeautifulSoup

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.models import Article
from helper.kafka_produce import make_producer, ArticleProducerCallback
from helper import schemas
from helper.cached_mongo import get_cached_articles

from tickets.tickets import TICKETS



# Constants for Kafka and API configurations
TOPIC_NAME = "test.articles_v2"
API_KEY = Variable.get("SEEK_ALPHA_API_KEY")
API_HOST = Variable.get("SEEK_ALPHA_API_HOST")
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")
SOURCE = 'seeking_alpha'

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKETS is a dict -> {stock_name: exchange}
tickets = TICKETS.keys()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dag(
    schedule="0 13 * * 2-6",  # Runs from Tuesday to Saturday at 13
    start_date=datetime(2024, 10, 5),
    catchup=False,
    tags=["stock_sentiment"]
)
def seeking_alpha_extract():
    """
    This DAG performs the following steps:
    1. Retrieves article links for stock from the Seeking Alpha API.
    2. Compares article titles with cached data to check for duplicates.
    3. Fetches article bodies and processes them if they are new.
    4. Produces the articles to a Kafka topic.
    5. Sends Telegram notifications on failure events.
    """


    @task(
        task_id='get_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def get_news_links_task(tickets, num=1):
        """
        Fetches article links for a list of stock tickers from the Seeking Alpha API.

        Args:
            tickets (list): A list of stock ticker symbols.
            num (int): The number of articles to fetch per ticker.
        
        Returns:
            list: A list of article metadata, including id, timestamp, title, and ticker.
        """
        url = "https://seeking-alpha.p.rapidapi.com/news/v2/list-by-symbol"
        articles_to_process = []
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }

        for ticket in tickets:
            querystring = {"size": num, "number": "1", "id": ticket}
            
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            data = response.json()
            
            articles_retrived = 0

            # try-except is for logging reasons
            try:
                # For details about the response visit:
                # https://rapidapi.com/apidojo/api/seeking-alpha/playground/apiendpoint_26d058f9-bc94-4bf6-b6d7-e405379006b3
                
                # For each article in data, get just the info I care about and append them
                for row in data['data']:
                    date_str = row['attributes']['publishOn']
                    date_time = datetime.fromisoformat(date_str)
                    unix_timestamp = int(date_time.timestamp())
                    
                    articles_to_process.append({
                        'id': row['id'],
                        'timestp': unix_timestamp,
                        'title': row['attributes']['title'],
                        'ticket': ticket.lower(),
                        'duplicate': False
                    })
                    
                    articles_retrived += 1
                    
            except Exception as e:
                logger.error(e)
                logger.error(f'The data sctructure is: {data}')
                raise
                
            logger.info(f"Successfully retrieved {articles_retrived} articles for ticket {ticket}.")

        return articles_to_process



    @task(
        task_id='check_for_duplicate',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def check_for_duplicate_task(articles_metadata):
        """
        Checks if the articles retrieved from the API are already in the cache.

        Args:
            articles_metadata (list): A list of article metadata.
        
        Returns:
            list: Updated list of article metadata, marking duplicates.
        """
        
        mongo_hook = MongoHook(conn_id='mongo_test')
        client = mongo_hook.get_conn()
        cached_articles = get_cached_articles(client=client, source=SOURCE)

        for article in articles_metadata:
            cached_title = cached_articles.get(article['ticket'].lower())
            if article['title'] == cached_title:
                article['duplicate'] = True

        logger.info(f"Checked {len(articles_metadata)} articles for duplicates.")
        return articles_metadata
        


    @task(
        task_id='process_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def process_links_task(articles_metadata):
        """
        Fetches article bodies for non-duplicate articles and produces them to a Kafka topic.

        Args:
            articles_metadata (list): List of dictionaries containing article metadata.
        """
        
        if not articles_metadata:
            logger.warning("No articles to process.")
            return
        
        url = "https://seeking-alpha.p.rapidapi.com/news/get-details"
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }

        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.article_schema_v2
        )
        logger.info("Kafka producer created successfully.")

        for raw_article in articles_metadata:
            if raw_article['duplicate'] == False:
                querystring = {"id": raw_article['id']}
                response = requests.get(url, headers=headers, params=querystring)
                response.raise_for_status()
                data = response.json()

                # For logging
                try:
                    # This is for extracting just the text, without html elemts like <p>
                    soup = BeautifulSoup(data['data']['attributes']['content'], 'html.parser')
                    article_content = soup.get_text()

                    article = Article(
                        ticket=raw_article['ticket'].lower(),
                        timestp=raw_article['timestp'],
                        url=data['data']['links']['canonical'],
                        title=raw_article['title'],
                        article_body=article_content,
                        source=SOURCE
                    )
                
                except Exception as e:
                    logger.error(e)
                    logger.error(f'The data sctructure is: {data}')
                    raise
                    
                logger.info(f"Processed article: {article.title}")

                producer.produce(
                    topic=TOPIC_NAME,
                    key=article.ticket,
                    value=article,
                    on_delivery=ArticleProducerCallback(article)
                )
                    
                logger.info(f"Produced article {article.title} to Kafka topic {TOPIC_NAME}.")
            
            else:
                logger.info(f'Found a duplicate article for {raw_article["ticket"]} -- {raw_article["title"]}')

        producer.flush()
        logger.info("Kafka producer flushed successfully.")
        
        
    telegram_failure_extract_msg = TelegramOperator(
        task_id='telegram_failure_extract_msg',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''I couldn't extract the link fron the SeekingAlpha API.
        
        The dag that failed is seeking_alpha_extract. The failed task is get_news_links_task.
        
        Probably, the API key is changed.''',
        trigger_rule='all_failed'
    )

    telegram_failure_process_msg = TelegramOperator(
        task_id='telegram_failure_process_msg',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''I couldn't get the articles body fron the SeekingAlpha API.
        
        The dag that failed is seeking_alpha_extract. The failed task is process_links_task.
        
        Idk what happened :(.''',
        trigger_rule='all_failed'
    )


    get_news_links = get_news_links_task(tickets)
    check_for_duplicate = check_for_duplicate_task(get_news_links)
    process_links = process_links_task(check_for_duplicate)
    
    get_news_links >> [check_for_duplicate, telegram_failure_extract_msg]
    check_for_duplicate >> [process_links, telegram_failure_process_msg]
    process_links >> telegram_failure_process_msg


seeking_alpha_extract()
