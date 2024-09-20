
from datetime import datetime, timedelta
import requests
import logging
from bs4 import BeautifulSoup

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator

from helper.models import Article
from helper.kafka_produce import make_producer, ArticleProducerCallback
from helper import schemas

from tickets.tickets import TICKETS



# Constants for Kafka and API configurations
TOPIC_NAME = "test.articles"
API_KEY = Variable.get("SEEK_ALPHA_API_KEY")
API_HOST = Variable.get("SEEK_ALPHA_API_HOST")
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKETS is a dict -> {stock_name: exchange}
tickets = TICKETS.keys()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def seeking_alpha_extract():
    """
    DAG to extract article data from the Seeking Alpha API, process the data,
    and produce it to a Kafka topic.
    """


    @task(
        task_id='get_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        depends_on_past=True
    )
    def get_news_links_task(tickets, num=1):
        """
        Task to retrieve news links for a specific stock ticker list from the Seeking Alpha API.
        
        Args:
            tickets (list): A list of stock ticker symbol.
            num (int): The number of articles to fetch.
        
        Returns:
            list: A list of dictionaries containing article metadata.
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

            # For details about the response visit:
            # https://rapidapi.com/apidojo/api/seeking-alpha/playground/apiendpoint_26d058f9-bc94-4bf6-b6d7-e405379006b3
            for row in data['data']:
                date_str = row['attributes']['publishOn']
                date_time = datetime.fromisoformat(date_str)
                unix_timestamp = int(date_time.timestamp())

                articles_to_process.append({
                    'id': row['id'],
                    'timestp': unix_timestamp,
                    'title': row['attributes']['title'],
                    'ticket': ticket
                })
                
            logger.info(f"Successfully retrieved {len(articles_to_process)} articles for ticker {ticket}.")

        return articles_to_process



    @task(
        task_id='process_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=30),
        trigger_rule='all_done'
    )
    def process_links_task(raw_articles):
        """
        Task to process raw article data, extract content, and produce it to a Kafka topic.
        
        Args:
            raw_articles (list): List of dictionaries containing raw article metadata.
        """
        
        # Useful to not mark the task as 'upstream_failed' when the one above fails. That's why trigger_rule='all_done'
        if not raw_articles:
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
            schema=schemas.article_schema_v1
        )
        logger.info("Kafka producer created successfully.")

        for raw_article in raw_articles:
            querystring = {"id": raw_article['id']}
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            data = response.json()

            soup = BeautifulSoup(data['data']['attributes']['content'], 'html.parser')
            article_content = soup.get_text()

            article = Article(
                ticket=raw_article['ticket'],
                timestp=raw_article['timestp'],
                url=data['data']['links']['canonical'],
                title=raw_article['title'],
                article_body=article_content
            )
                
            logger.info(f"Processed article: {article.title}")

            producer.produce(
                topic=TOPIC_NAME,
                key=article.ticket.lower(),
                value=article,
                on_delivery=ArticleProducerCallback(article)
            )
                
            logger.info(f"Produced article {article.title} to Kafka topic {TOPIC_NAME}.")

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
    process_links = process_links_task(get_news_links)
    
    get_news_links >> [process_links, telegram_failure_extract_msg]
    process_links >> telegram_failure_process_msg


seeking_alpha_extract()
