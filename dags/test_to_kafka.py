from datetime import datetime, timedelta
from airflow.decorators import dag, task

import requests
import logging
from bs4 import BeautifulSoup
from airflow.hooks.base import BaseHook

from helper.models import Article
from helper.kafka_produce import make_producer, ProducerCallback
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from helper import schemas
from confluent_kafka.serialization import StringSerializer





TOPIC_NAME = "test.articles.airflow"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def prod_func(peppa):
    print(peppa)
    kafka_conn = BaseHook.get_connection('kafka_default')
    print('_____________________________')
    print(f"Extra: {kafka_conn.extra}")
    print('_____________________________')
    article = Article(
            ticket='Peppa',
            timestp=7,
            article_body="I'm Peppa Pig.",
            title='Papa che e',
            url='testing'
        )
        
    return article
    


@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def test_extract():
    
    @task(
        task_id='get_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def get_news_links_task(ticket='RIOT', num=1):
        
        articles_to_process = {}

        return "This is an xcom."


    produce_article = ProduceToTopicOperator(
        task_id="produce_article",
        kafka_config_id='kafka_default',
        topic=TOPIC_NAME,
        producer_function=prod_func,
        producer_function_kwargs={"peppa": "{{ ti.xcom_pull(task_ids='get_the_links')}}"},
        poll_timeout=10,
    )

                      


    
    get_news_links_task() >> produce_article
                
test_extract()