import os
from openai import OpenAI
import logging

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import schemas
from models import Article


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "stock.news.v1"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
CONSUMER_GROUP='stock.news.v1-grp-0'


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def make_consumer() -> DeserializingConsumer:
    # create a SchemaRegistryClient
    schema_reg_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    # create an AvroDeserializer
    avro_deserializer = AvroDeserializer(schema_reg_client,
                                         schemas.article_schema_v1,
                                         lambda data, ctx: Article(**data))
    
    # create and return DeserializingConsumer
    return DeserializingConsumer({'bootstrap.servers': BOOTSTRAP_SERVERS,
                                  'key.deserializer': StringDeserializer('utf-8'),
                                  'value.deserializer': avro_deserializer,
                                  'group.id': CONSUMER_GROUP,
                                  'enable.auto.commit': 'false'})


def main():
    client = OpenAI(api_key=os.environ['OPENAI_KEY'])
    logger.info(f"""
                Started Python Avro Consumer
                for topic {TOPIC_NAME}
                """)
    
    consumer = make_consumer()
    consumer.subscribe([TOPIC_NAME])
    
    while True:
        msg = consumer.poll(1.0)
        if msg is not None:
            article = msg.value()
            logger.info(f"Consumed article for stock {article.ticket}")
            
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are the best stock analyzer."},
                    {
                        "role": "user",
                        "content": f"Analyze this article about the stock {article.ticket} and write a small summary and one line of the stock sentiment: \n{article.article_body}."
                    }
                ]
            )
            
            print(completion.choices[0].message)
            consumer.commit(message=msg)


if __name__ == '__main__':
    main()