
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.models import StockSentiment
from helper.kafka_produce import make_producer, GenralProducerCallback
from helper import schemas
from helper.for_openai_api import get_info_from_mongo, get_sentiment

from tickets.tickets import TICKETS



# Constants for Kafka and API configurations
TOPIC_NAME = "test.openai_sentiment"
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKETS is a dict -> {stock_name: exchange}
tickets = list(TICKETS.keys())[0] # for testing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


mongo_hook = MongoHook(conn_id='mongo_test')
    
producer = make_producer(
    schema_reg_url=SCHEMA_REGISTRY_URL,
    bootstrap_server=BOOTSTRAP_SERVERS,
    schema=schemas.balance_sheet_schema_v1
)


@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def stock_sentiment_analysis():
    """
    DAG to make a call to openai and get the stock sentiment + prediction, produce them to Kafka e save to MongoDB.
    """

    @task(
        task_id='process_ticket',
        retries=0,
        retry_delay=timedelta(seconds=5),
        depends_on_past=True
    )
    def process_ticket_task(ticket):
        """
        This retrives info about a stock ticket (sentiment, prices, balance sheet) and send it to
        openai for an analysis. The result is produces to kafka
        
        Args:
            ticket (str): A of stock ticker symbol.
            producer (SerializingProducer): Producer to send messages to Kafka.
            mongo_hook (MongoHook): Hook to access Mongo db 
        """
        
        # Retrives the data needed
        articles = get_info_from_mongo(collection='articles_test', ticket=ticket, limit=3, mongo_hook=mongo_hook)
        logger.info(f'Successfully retrived articles data for {ticket}')
        
        prices = get_info_from_mongo(collection='price_info', ticket=ticket, limit=1, mongo_hook=mongo_hook)
        logger.info(f'Successfully retrived prices data for {ticket}')
        
        balance_sheet = get_info_from_mongo(collection='balance_sheet', ticket=ticket, limit=1, mongo_hook=mongo_hook)
        logger.info(f'Successfully retrived balance_sheet data for {ticket}')
        
        # Create OpenAI message
        openai_message = f'''
        Below, you'll find articles, daily price, and the balance sheet about the stock {ticket}. 
        Analyze them and give a score to the stock sentiment from 1 to 100.
        Write a comprehensive reasoning explaining the score.
        
        Articles: {articles}
        Prices: {prices}
        Balance sheet: {balance_sheet}
        '''
        
        sentiment = get_sentiment(openai_message=openai_message, format=StockSentiment)
        sentiment.ticket = ticket.lower()
        logger.info(f'Successfully retrived sentiment data for {ticket}')
            
        producer.produce(
            topic=TOPIC_NAME,
            key=ticket.lower(),
            value=sentiment,
            on_delivery=GenralProducerCallback(sentiment)
        )
        logger.info(f'Successfully produced message to Kafka for {ticket}')

        
    telegram_failure_msg = TelegramOperator(
        task_id='telegram_failure_msg',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''Hey there, looks like I ran into some trouble getting sentiment data for all the tickets.
        
        Something didn’t go as planned.''',
        trigger_rule='all_failed'
    )
    


    process_ticket= process_ticket_task.expand(ticket=tickets)
    
    process_ticket >> telegram_failure_msg
    
    producer.flush()


stock_sentiment_analysis()
