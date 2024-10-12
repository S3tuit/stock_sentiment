
from datetime import datetime, timedelta
import requests
import logging
from bs4 import BeautifulSoup

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.models import Article
from helper.kafka_produce import make_producer, ArticleProducerCallback
from helper import schemas
from helper.cached_mongo import get_cached_articles

from tickers.tickers import TICKERS



# Constants for Kafka and API configurations
TOPIC_NAME = "test.articles_v2"
API_KEY = Variable.get("SEEK_ALPHA_API_KEY")
API_HOST = Variable.get("SEEK_ALPHA_API_HOST")
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")
SOURCE = 'seeking_alpha'

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKERS is a dict -> {stock_name: exchange}
tickers = TICKERS.keys()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dag(
    schedule=None,
    start_date=datetime(2023, 10, 5),
    catchup=False,
    tags=["stock_sentiment", "testing"]
)
def test_seeking_alpha_extract():
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
    def get_news_links_task():
        telegram_hook = TelegramHook('telegram_conn')
        telegram_hook.send_message({"text": "message_test", "chat_id": chat_id})
        
    
    get_news_links_task()
    
test_seeking_alpha_extract()