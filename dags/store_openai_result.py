
from datetime import datetime, timedelta
import requests
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from helper.models import Article
from helper.kafka_produce import make_producer, ArticleProducerCallback
from helper import schemas

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



@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def seeking_alpha_extract():
    """
    DAG to make a call to openai and get the stock sentiment + prediction, produce them to Kafka e save to MongoDB.
    """


    @task(
        task_id='prepare_the_info',
        retries=0,
        retry_delay=timedelta(seconds=5),
        depends_on_past=True
    )
    def prepare_the_info_task(tickets, num=1):
        """
        Task to retrieve news links for a specific stock ticker list from the Seeking Alpha API.
        
        Args:
            tickets (list): A list of stock ticker symbol.
            num (int): The number of articles to fetch.
        
        Returns:
            list: A list of dictionaries containing article metadata.
        """
        mongo_hook = MongoHook(conn_id='mongo_test')
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
                        'ticket': ticket
                    })
                    
                    articles_retrived += 1
                    
            except Exception as e:
                logger.error(e)
                logger.error(f'The data sctructure is: {data}')
                raise
                
            logger.info(f"Successfully retrieved {articles_retrived} articles for ticket {ticket}.")

        return articles_to_process


        
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
