from helper.models import Article
import helper.schemas as schemas
import logging

from confluent_kafka import SerializingProducer

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer





logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# helper function to create a producer
def make_producer(schema_reg_url, bootstrap_server, schema) -> SerializingProducer:
    # make a SchemaRegistryClient
    schema_reg_client = SchemaRegistryClient({'url': schema_reg_url})
    
    # create AvroSerializer
    avro_serializer = AvroSerializer(schema_reg_client,
                                     schema,
                                     lambda article, ctx: article.dict())
    
    # create and return SerializingProducer
    return SerializingProducer({'bootstrap.servers': bootstrap_server,
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
            print(f"Failed to produce article on url: {self.article.url} \nFor the stock: {self.article.ticket}", exc_info=err)
        else:
            print(f"""
                        __________________________________
                        Successfully produced article on url: {self.article.url}
                        to partition {msg.partition()}
                        at offset {msg.offset()}
                        __________________________________
                        """)