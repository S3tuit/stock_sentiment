
from datetime import datetime, timedelta
import requests
import logging
from time import sleep

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.models import BalanceSheet
from helper.kafka_produce import make_producer, GenralProducerCallback
from helper import schemas
from helper.cached_mongo import get_latest_balance_time

from tickers.tickers import TICKERS



# Constants for Kafka and API configurations
TOPIC_NAME = "test.balance_sheet"
API_KEY = Variable.get("ALPHA_VANTAGE_API_KEY")
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Parameters for Telegram bot
chat_id = Variable.get("TELEGRAM_CHAT")

# TICKERS is a dict -> {stock_name: exchange}
tickers = list(TICKERS.keys())

# How many days before making another call to update the balance sheet
DAYS_TO_WAIT = 120

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dag(
    schedule="0 11 * * 1",  # Runs each Monday at 11 UTC
    start_date=datetime(2024, 10, 5),
    catchup=True,
    tags=["stock_sentiment"]
)
def balance_sheet_extract():
    """
    DAG to extract balance sheet and earnings data from the AlphaVantage API, keep the useful info,
    and produce it to a Kafka topic.
    """


    @task.branch(
        task_id='is_api_available',
        retries=1,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(seconds=30)
    )
    def is_api_available_task(ticker):
        """
        Branch operator to check wheter the AlphaVantage API is up and the key is correct.
        
        Args:
            ticker (str): A stock ticker symbol.
        
        Returns:
            task: 'balance_sheet_extract' if the api is up, 'telegram_api_down' otherwise.
        """
        
        try:
            url_to_test = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
            response = requests.get(url_to_test)
            
            response.raise_for_status()
            
            return 'is_too_early'
            
        except Exception as e:
            logger.error(e)
            return 'telegram_api_down'



    @task(
        task_id='is_too_early',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def is_too_early_task(tickers):
        mongo_hook = MongoHook(conn_id='mongo_test')
        client = mongo_hook.get_conn()
        
        now_minus_120_days = int(datetime.now().timestamp()) - (DAYS_TO_WAIT * 24 * 60 * 60)
        
        too_old_balance_sheet = get_latest_balance_time(client=client, tickers=tickers, timestp_threshold=now_minus_120_days)
        
        if not too_old_balance_sheet:
            logger.info(f"There's no balance sheet retrived before {DAYS_TO_WAIT} days from now.")
            
            return ['all_up_to_date']
        
        return too_old_balance_sheet      
        
        

    @task(
        task_id='balance_sheet_extract',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def balance_sheet_extract_task(tickers):
        """
        Task to get balance sheet and earnings data, extract useful info, and produce it to a Kafka topic.
        
        Args:
            tickers (list): List of stock ticker symbols.
        """
        
        if not tickers:
            logger.error("*tickers* is NULL, no tickers to process.")
            raise
        
        if tickers[0] == 'all_up_to_date':
            logger.info(f"There's no balance sheet retrived before {DAYS_TO_WAIT} days from now.")
            return
        
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.balance_sheet_schema_v1
        )
        logger.info("Kafka producer created successfully.")
        
        # try-except is for logging reasons and to flush the successful messages even if there's an error
        try:
            for ticker in tickers:
                
                earnings = 'None'
                balance_sheet = 'None'
                balance_sheet_model = 'None'
                
                # Get the earnings and ratios
                earnings_useful = ['MarketCapitalization', 'DilutedEPSTTM', 'PERatio', 'ForwardPE', 'EPS', 'RevenueTTM', 'QuarterlyRevenueGrowthYOY',
                                'ProfitMargin', 'OperatingMarginTTM', 'Beta', 'PriceToSalesRatioTTM', 'PriceToBookRatio']
                url_earnings = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
                earnings = requests.get(url_earnings).json()
                # Keep just the info I'm interested in
                earnings = {key: earnings.get(key) for key in earnings_useful}
                
                
                # Get the balance_sheet
                balance_sheet_useful = ['totalAssets', 'cashAndCashEquivalentsAtCarryingValue', 'totalLiabilities', 'totalShareholderEquity',
                                        'retainedEarnings', 'cashAndShortTermInvestments', 'propertyPlantEquipment', 'commonStockSharesOutstanding',
                                        'longTermDebt', 'currentDebt', 'shortTermDebt']
                balance_sheet_url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker}&apikey={API_KEY}'
                balance_sheet = requests.get(balance_sheet_url).json()
                # Get the last report
                balance_sheet = balance_sheet['quarterlyReports'][0]
                    
                # Convert the day of the quarterly report into a datetime.datetime, next it'll become unix time
                fiscal_date = balance_sheet['fiscalDateEnding']
                datetime_fiscal_date = datetime.strptime(fiscal_date, "%Y-%m-%d")
                    
                # Keep just the info I'm interested in
                balance_sheet = {key: balance_sheet.get(key) for key in balance_sheet_useful}                
                
                logger.info(f"Successfully retrived info for: {ticker}")
                
                
                balance_sheet_model = BalanceSheet(
                    ticker=ticker.lower(),
                    timestp=int(datetime_fiscal_date.timestamp()),
                    earnings_ratios=earnings,
                    balance_sheet=balance_sheet
                )
            
                logger.info(f"Successfully processed balance_sheet for: {ticker}")

                producer.produce(
                    topic=TOPIC_NAME,
                    key=balance_sheet_model.ticker,
                    value=balance_sheet_model,
                    on_delivery=GenralProducerCallback(balance_sheet_model)
                )
                    
                logger.info(f"Produced {ticker}'s balance sheet report to Kafka topic {TOPIC_NAME}.")
                
                # This will reduce the strain on the free service (tnx)
                sleep(5)
        except Exception as e:
            logger.error(e)
            logger.error(f'The earnings sctructure is: {earnings}')
            logger.error(f'The balance_sheet sctructure is: {balance_sheet}')
            logger.error(f'The balance_sheet MODEL sctructure is: {balance_sheet_model}')
            
            producer.flush()
            raise

        producer.flush()
        logger.info("Kafka producer flushed successfully.")
        
        
    telegram_api_down = TelegramOperator(
        task_id='telegram_api_down',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''The dag balance_sheet_extract failed. The task that failed is balance_sheet_extract.'''
    )

    telegram_failure_msg = TelegramOperator(
        task_id='telegram_failure_msg',
        telegram_conn_id='telegram_conn',
        chat_id=chat_id,
        text='''The dag balance_sheet_extract failed. The task that failed is is_too_early.''',
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


    is_api_available = is_api_available_task(tickers[0])
    is_too_early = is_too_early_task(tickers)
    balance_sheet = balance_sheet_extract_task(is_too_early)
    mark_dag_as_failed = mark_dag_as_failed_task()
    
    is_api_available >> [telegram_api_down, is_too_early]
    is_too_early >> [balance_sheet, telegram_failure_msg]
    balance_sheet >> telegram_failure_msg
    [telegram_failure_msg, telegram_api_down] >> mark_dag_as_failed


balance_sheet_extract()
