
from models import Article
import helper

import logging
from fastapi import FastAPI
from typing import List

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import schemas
from models import Article


HEADERS = {
'article-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36 Edg/85.0.564.44'
}
TICKET = 'riot'
PREFIXES_FOOL = [r'https://www.fool.com/quote/nasdaq/',
                r'https://www.fool.com/quote/nyse/']

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "stock.news.v1"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


app = FastAPI()


# create the topic if it doesn't exist
@app.on_event('startup')
async def startup_event():
    client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    topic = NewTopic(TOPIC_NAME,
                     num_partitions=1,
                     replication_factor=1)
    
    # check if the topic already exists or create a new one
    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Create topic {topic_name}")
    except Exception as e:
        logger.warning(e)


# helper function to create a producer
def make_producer() -> SerializingProducer:
    # make a SchemaRegistryClient
    schema_reg_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    # create AvroSerializer
    avro_serializer = AvroSerializer(schema_reg_client,
                                     schemas.article_schema_v1,
                                     lambda article, ctx: article.dict())
    
    # create and return SerializingProducer
    return SerializingProducer({'bootstrap.servers': BOOTSTRAP_SERVERS,
                                'linger.ms': 300,
                                'enable.idempotence': 'true',
                                'acks': 'all',
                                'key.serializer': StringSerializer('utf-8'),
                                'value.serializer': avro_serializer,
                                'partitioner': 'murmur2_random'})


# class to manage callbacks when the producer writes messages to Kafka
class ProducerCallback:
    def __init__(self, article):
        self.article = article
        
    def __call__(self, err, msg):
        if err:
            logger.error(f"Failed to produce article on url: {self.article.url} \nFor the stock: {self.article.ticket}", exc_info=err)
        else:
            logger.info(f"""
                        __________________________________
                        Successfully produced article on url: {self.article.url}
                        to partition {msg.partition()}
                        at offset {msg.offset()}
                        __________________________________
                        """)

# make a call for each iteration to https://randomuser.me/api/
@app.post('/api/news', status_code=201, response_model=List[str])
async def get_stock_news(ticket: str) -> List[str]:

    producer = make_producer()
    fool_links = []
    
    # get the links
    for prefix in PREFIXES_FOOL:
        try:
            url = prefix + ticket + '/'
            links = helper.fool_get_news_links(url, HEADERS)
            
            # if the function finds no link it stops
            if links:
                fool_links = fool_links + links
        
        except Exception as e:
            print('Something whent wrong :(. ', e)
    
    article = helper.extract_articles(headers=HEADERS, ticket=ticket)
    processed_urls = []
    
    
    for fool_link in fool_links:
        
        try:
            article = helper.fool_get_article_info(url=fool_link, headers=HEADERS, ticket=ticket)
            
            producer.produce(topic=TOPIC_NAME,
                            key=article.ticket.lower(),
                            value=article,
                            on_delivery=ProducerCallback(article))
            processed_urls.append(fool_link)
            
        except Exception as e:
            print(f"Something went wrong while extracting the url: {fool_link}", e)
            
    
    producer.flush()
    
    return processed_urls