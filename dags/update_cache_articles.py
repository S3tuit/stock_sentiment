
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.cached_mongo import get_latest_articles, upsert_articles


# Constants for Kafka and API configurations
TOPIC_NAME = "test.articles_v2"
API_KEY = Variable.get("SEEK_ALPHA_API_KEY")
API_HOST = Variable.get("SEEK_ALPHA_API_HOST")
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dag(
    schedule="30 11 * * 2-6",  # Runs from Tuesday to Saturday at 11 UTC
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=["stock_sentiment"]
)
def update_cache_articles():
    """
    This DAG performs the following tasks:
    1. Checks if MongoDB is up and accessible.
    2. Retrieves the latest article data from the Seeking Alpha API.
    3. Performs an upsert operation to update the MongoDB cache with the latest articles.
    4. Sends Telegram notifications for any errors.
    """


    @task.branch(
        task_id='is_mongo_up',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def is_mongo_up_task():
        """
        Checks if MongoDB is up and running by attempting to query the 'articles_test' collection.
        Returns a branch to either proceed with the update or notify that Mongo is down.
        """
        
        try:
            mongo_hook = MongoHook(conn_id='mongo_test')
            client = mongo_hook.get_conn()
            db = client['stock_test']
            test_collection = db['articles_test']

            # Attempt to find a random document
            result = test_collection.find_one()

            if result:
                logger.info("MongoDB is up and accessible.")
                return 'update_cache'
            else:
                logger.warning("MongoDB is up, but no documents found in 'articles_test'.")
                return 'mongo_down' 
        except Exception as e:
            logger.error(f"Error accessing MongoDB: {e}")
            return 'mongo_down'    


    @task(
        task_id='update_cache',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def update_cache_task():
        """
        Retrieves the latest articles from various sources and performs an upsert in MongoDB's cache.
        """
        
        mongo_hook = MongoHook(conn_id='mongo_test')
        client = mongo_hook.get_conn()
        
        for source in ['seeking_alpha', 'motley_fool']:
            latest_articles = get_latest_articles(client=client, source=source)
            logger.info(f"Retrieved {len(latest_articles)} articles from {source}.")
            
            upsert_articles(article_entities=latest_articles, client=client)
            logger.info(f"Successfully upserted {source} articles into MongoDB cache.")
        
        
    mongo_down = TelegramOperator(
        task_id='mongo_down',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''The dag update_cache_articles failed. That's likely because MongoDB is down.''',
        trigger_rule='one_failed'
    )

    unexpected_error_upsert = TelegramOperator(
        task_id='unexpected_error_upsert',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''The dag update_cache_articles failed. The task that failed is update_cache.''',
        trigger_rule='one_failed'
    )
    
    @task(
        task_id='mark_dag_as_failed',
        retries=0,
        retry_delay=timedelta(seconds=5),
        trigger_rule='one_success'
    )
    def mark_dag_as_failed_task():
        """
        Task to mark the dag as failed.
        """
        raise


    is_mongo_up = is_mongo_up_task()
    update_cache = update_cache_task()
    mark_dag_as_failed = mark_dag_as_failed_task()
    
    is_mongo_up >> [update_cache, mongo_down]
    update_cache >> unexpected_error_upsert
    [mongo_down, unexpected_error_upsert] >> mark_dag_as_failed


update_cache_articles()
