
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.kafka_produce import make_producer, GenralProducerCallback
from helper import schemas
from helper.for_openai_api import get_info_from_mongo, get_sentiment

from tickers.tickers import TICKERS


# Constants for Kafka and API configurations
TOPIC_NAME = "test.openai_sentiment"
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKERS is a dict -> {stock_name: exchange}
tickers = list(TICKERS.keys())

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# For openai API
OPENAI_API_KEY = Variable.get("OPENAI_API_KEY")


@dag(
    schedule="0 13 * * 6",  # Runs each Saturday at 13 UTC
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=["stock_sentiment"],
    max_active_tasks=1,  # This limits the DAG to run one task at a time
    default_args={
        "max_active_tis_per_dag": 1  # This ensures only one task instance runs at any point, otherwise pc crushes :(
    }
)
def store_openai_result():
    """
    DAG to make a call to openai and get the stock sentiment + prediction, produce them to Kafka e save to MongoDB.
    """

    @task(
        task_id='process_ticket',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def process_ticket_task(ticker):
        """
        This retrives info about a stock ticker (sentiment, prices, balance sheet) and send it to
        openai for an analysis. The result is produces to kafka.
        
        Args:
            ticker (str): A of stock ticker symbol.
        """
        mongo_hook = MongoHook(conn_id='mongo_test')
            
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.stock_sentiment_schema_v1
        )
        

        # Retrives the data needed
        articles = get_info_from_mongo(collection='articles_test', ticker=ticker.lower(), limit=3, mongo_hook=mongo_hook)
        logger.info(f'Successfully retrived articles data for {ticker}')
        
        prices = get_info_from_mongo(collection='price_info', ticker=ticker.lower(), limit=6, mongo_hook=mongo_hook)
        logger.info(f'Successfully retrived prices data for {ticker}')
        
        balance_sheet = get_info_from_mongo(collection='balance_sheet', ticker=ticker.lower(), limit=1, mongo_hook=mongo_hook)
        logger.info(f'Successfully retrived balance_sheet data for {ticker}')
        
        
        # Extract the body of the articles
        article_bodies = ''
        for article in articles:
            article_bodies += article['article_body'] + '\n'
            
        # Extract the useful info from prices
        price_n_volume = prices[0]['price_n_volume']
        technicals = prices[0]['technicals']
        # Usually 5 trading days ago is a week ago
        price_n_volume_last_week = prices[5]['price_n_volume']
        technicals_last_week = prices[5]['technicals']
        
        # Extract the useful info from the balance sheet
        earnings_ratios = balance_sheet[0]['earnings_ratios']
        balance_sheet = balance_sheet[0]['balance_sheet']
        
        
        # Create OpenAI message
        openai_message = f'''
        Below, you'll find articles, daily price, and the balance sheet about the stock {ticker}. 
        Analyze them and make a price prediction for the next month and next year.
        Write a comprehensive reasoning explaining the score.
        
        Articles: {article_bodies}
        Prices: {price_n_volume}
        Technicals: {technicals}
        Last week prices: {price_n_volume_last_week}
        Technicals: {technicals_last_week}
        Ratios: {earnings_ratios}
        Balance sheet: {balance_sheet}
        '''

        sentiment = get_sentiment(openai_message=openai_message, api_key=OPENAI_API_KEY, ticker=ticker, logger=logger)
        logger.info(f'Successfully retrived sentiment data for {ticker}')
            
        producer.produce(
            topic=TOPIC_NAME,
            key=ticker.lower(),
            value=sentiment,
            on_delivery=GenralProducerCallback(sentiment)
        )
        
        logger.info(f'Successfully produced message to Kafka for {ticker}')
        producer.flush()
        logger.info(f'Successfully flushed message to Kafka for {ticker}')

        
    telegram_failure_msg = TelegramOperator(
        task_id='telegram_failure_msg',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''The dag store_openai_result failed. The task that failed is process_ticket.''',
        trigger_rule='all_failed'
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
    


    process_ticket = process_ticket_task.expand(ticker=tickers)
    mark_dag_as_failed = mark_dag_as_failed_task()
    
    process_ticket >> telegram_failure_msg
    telegram_failure_msg >> mark_dag_as_failed
    


store_openai_result()
