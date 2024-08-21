
from helper.models import Article
import helper.schemas as schemas
from helper.yahoo import fetch_yahoo, fetch_yahoo_article_content

import asyncio
from typing import List
import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# Define variables
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
}
cookies = {
    'GUC': 'AQABCAFmwcNm80IfWgSU&s=AQAAAGoqDQ7v&g=ZsB0FA',
    'A1': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'A3': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'A1S': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'PRF': 't%3DACMR%26newChartbetateaser%3D0%252C1725284010825'
}

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "test.articles"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


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


async def get_stock_news(tickets):

    producer = make_producer()
    
    for ticket in tickets:
        # Get the tasks to run
        articles = await fetch_yahoo(ticket=ticket, headers=HEADERS, cookies=cookies)
        
        # Run the tasks
        for article in articles:
            if article:
                producer.produce(topic=TOPIC_NAME,
                                key=article.ticket.lower(),
                                value=article,
                                on_delivery=ProducerCallback(article))
        
        producer.flush()
        
tickets = ['ACMR', 'RIOT']
asyncio.run(get_stock_news(tickets))