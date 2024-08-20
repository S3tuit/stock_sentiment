from datetime import datetime, timedelta
from airflow.decorators import dag, task

import requests
import logging
from bs4 import BeautifulSoup

from helper.models import Article
from helper.kafka_produce import make_producer, ProducerCallback
from helper import schemas
from confluent_kafka.serialization import StringSerializer


from confluent_kafka import Producer


# Find them at https://rapidapi.com/apidojo/api/seeking-alpha/playground
API_KEY = '3f7b06e7a8msh390b7f13312554ep1ecb05jsnbae1915ac3c3'
API_HOST = "seeking-alpha.p.rapidapi.com"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "test.articles"
# SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()



@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def seeking_alpha_extract():
    
    @task(
        task_id='get_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def get_news_links_task(ticket='RIOT', num=1):
        url = "https://seeking-alpha.p.rapidapi.com/news/v2/list-by-symbol"
        querystring = {"size": num, "number": "1", "id": ticket}
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }
        
        articles_to_process = {}

        # try:
        #     response = requests.get(url, headers=headers, params=querystring)
        #     response.raise_for_status()
        #     data = response.json()

        #     for row in data['data']:
        #         date_str = row['attributes']['publishOn']
        #         date_time = datetime.fromisoformat(date_str)
        #         unix_timestamp = int(date_time.timestamp())

        #         articles_to_process[row['id']] = {
        #             'timestp': unix_timestamp,
        #             'title': row['attributes']['title'],
        #             'ticket': ticket
        #         }
        # except requests.RequestException as e:
        #     logger.error(f"HTTP Request failed: {e}")

        return articles_to_process


    @task(
        task_id='process_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=30)
    )
    def process_links_task(articles):
        url = "https://seeking-alpha.p.rapidapi.com/news/get-details"
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }
        
        # producer = make_producer(
        #             schema_reg_url=SCHEMA_REGISTRY_URL,
        #             bootstrap_server=BOOTSTRAP_SERVERS,
        #             schema=schemas.article_schema_v1
        #         )
        
        producer_config = {
            'bootstrap.servers': BOOTSTRAP_SERVERS
        }
        producer = Producer(producer_config)
        print('Producer created.')
        
        article = Article(
            ticket='Peppa',
            timestp=7,
            article_body="I'm Peppa Pig.",
            title='Papa che e',
            url='testing'
        )
        
        print('---------------')
        print(article)
        print('---------------')
        producer.produce(
                    topic=TOPIC_NAME,
                    key=article.ticket.lower(),
                    value=article
                )
                
        print('Article produced.')

        print("Flushing.")
        producer.flush()
        print("Flushed.")

        # try:
        #     for id in articles:
                
        #         producer = make_producer(
        #             schema_reg_url=SCHEMA_REGISTRY_URL,
        #             bootstrap_server=BOOTSTRAP_SERVERS,
        #             schema=schemas.article_schema_v1
        #         )
        #         print('Producer created.')
                
        #         querystring = {"id": id}
        #         response = requests.get(url, headers=headers, params=querystring)
        #         response.raise_for_status()
        #         data = response.json()

        #         soup = BeautifulSoup(data['data']['attributes']['content'], 'html.parser')
        #         article_content = soup.get_text()

        #         article = Article(
        #             ticket=articles[id]['ticket'],
        #             timestp=articles[id]['timestp'],
        #             url=data['data']['links']['canonical'],
        #             title=articles[id]['title'],
        #             article_body=article_content
        #         )
                
        #         print('Article processed:')
        #         print('---------------------------------------------')
        #         print(article)
        #         print('---------------------------------------------')

        #         producer.produce(
        #             topic=TOPIC_NAME,
        #             key=article.ticket.lower(),
        #             value=article,
        #             on_delivery=ProducerCallback(article)
        #         )
                
        #         print('Article produced.')

        #         print("Flushing.")
        #         producer.flush()
        #         print("Flushed.")
        # except requests.RequestException as e:
        #     logger.error(f"HTTP Request failed: {e}")
        # except Exception as e:
        #     logger.error(f"Failed to process links: {e}")                           


    
    articles = get_news_links_task()
    process_links_task(articles)
                
seeking_alpha_extract()