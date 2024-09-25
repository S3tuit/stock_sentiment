
from datetime import datetime, timedelta
import requests
import logging
from time import sleep

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator

from helper.models import Prices
from helper.kafka_produce import make_producer, GenralProducerCallback
from helper import schemas

from tickets.tickets import TICKETS



# Constants for Kafka and API configurations
TOPIC_NAME = "test.price_info"
API_KEY = Variable.get("ALPHA_VANTAGE_API_KEY")
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKETS is a dict -> {stock_name: exchange}
tickets = list(TICKETS.keys())

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def price_extract():
    """
    DAG to extract daily price daat from the AlphaVantage API, keep the useful info,
    and produce it to a Kafka topic.
    """


    @task.branch(
        task_id='is_api_available',
        retries=1,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(seconds=30),
        depends_on_past=True
    )
    def is_api_available_task(ticket):
        """
        Branch operator to check wheter the AlphaVantage API is up and the key is correct.
        
        Args:
            ticket (str): A stock ticker symbol.
        
        Returns:
            task: 'get_the_prices' if the api is up, 'telegram_api_down' otherwise.
        """
        try:
            url_to_test = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticket}&apikey={API_KEY}'
            response = requests.get(url_to_test)
            
            response.raise_for_status()
            
            return 'get_the_prices'
            
        except Exception as e:
            logger.error(e)
            return 'telegram_api_down'



    @task(
        task_id='get_the_prices',
        retries=0,
        retry_delay=timedelta(seconds=5),
        trigger_rule='all_done'
    )
    def get_the_prices_task(tickets):
        """
        Task to get daily price data, extract useful info, and produce it to a Kafka topic.
        
        Args:
            tickets (list): List of stock ticket symbols.
        """
        
        if not tickets:
            logger.error("tickets is NULL, no tickets to process.")
            raise
        
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.prices_schema_v1
        )
        logger.info("Kafka producer created successfully.")
        
        for ticket in tickets:
            
            # For logging 
            try:
                daily_price = 'None'
                # Get the price and volume of the last trading day
                url_daily_price = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticket}&apikey={API_KEY}'
                daily_price = requests.get(url_daily_price).json()
                daily_price = daily_price['Global Quote']
                
                # Convert the last trading day into a datetime.datetime, next it'll become unix time
                trading_day = daily_price['07. latest trading day']
                datetime_trading_day = datetime.strptime(trading_day, "%Y-%m-%d")
            
            except Exception as e:
                logger.error(e)
                logger.error(f'The daily price sctructure is: {daily_price}')
                raise
            
            try:
                technicals = 'None'
                # Get technical values of the last trading day and keep just the useful (for this purpose) ones
                technicals_useful = ['52WeekHigh', '52WeekLow', '50DayMovingAverage', '200DayMovingAverage']
                url_technicals = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticket}&apikey={API_KEY}'
                technicals = requests.get(url_technicals).json()
                technicals = {key: technicals.get(key) for key in technicals_useful}
            except Exception as e:
                logger.error(e)
                logger.error(f'The technicals sctructure is: {technicals}')
                raise
            
            logger.info(f"Successfully retrived info for: {ticket}")
            
            
            price = Prices(
                ticket=ticket.lower(),
                timestp=int(datetime_trading_day.timestamp()),
                price_n_volume=daily_price,
                technicals=technicals
            )
        
            logger.info(f"Successfully processed price for: {ticket}")

            producer.produce(
                topic=TOPIC_NAME,
                key=price.ticket.lower(),
                value=price,
                on_delivery=GenralProducerCallback(price)
            )
                
            logger.info(f"Produced price info about {ticket} to Kafka topic {TOPIC_NAME}.")
            
            # This will reduce the strain on the free service (tnx)
            sleep(1)

        producer.flush()
        logger.info("Kafka producer flushed successfully.")
        
        
    telegram_api_down = TelegramOperator(
        task_id='telegram_api_down',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''I couldn't extract price info from the AlphaVantage API.
        
        The API is unavailable or the key is changed.'''
    )

    telegram_failure_msg = TelegramOperator(
        task_id='telegram_failure_msg',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''Something unexpected happened when trying to get the daily price via AlphaVantage API.
        
        The dag that failed is price_extract. The failed task is get_the_prices_task.''',
        trigger_rule='all_failed'
    )


    is_api_available = is_api_available_task(tickets[0])
    get_the_prices = get_the_prices_task(tickets)
    
    is_api_available >> [telegram_api_down, get_the_prices]
    get_the_prices >> telegram_failure_msg


price_extract()
