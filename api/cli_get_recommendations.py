import logging
from typing import List
from fastapi import FastAPI
import time
import httpx
from asyncio import sleep
import pprint

from confluent_kafka import SerializingProducer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


from api_models import Message, TicketRequest
from helper.api_functions import send_to_open_ai, get_article





BOOTSTRAP_SERVERS = "broker:29092"
TOPIC_NAME = "test.recommendations"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# configurations used for the produce
conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


app = FastAPI()


# make a call for each iteration to https://randomuser.me/api/
@app.post('/api/recomm', status_code=201, response_model=Message)
def get_recommendation(request: TicketRequest):
    
    ticket = request.ticket
        
    # Fetch article for the given ticket
    article = get_article(ticket)
    
    if not article:
        logger.warning(f"There's no such ticket: {ticket}")
        return
    
    article_body = article['article_body']
    
    if not article_body:
        logger.warning(f"There's no article for the ticket: {ticket}")
        return
    
    # Prepare the message for OpenAI
    role = f"You're the best stock analyst. You'll be given articles about the stock {ticket}. Rate it from 0 to 100, give a reasoning and adhere at the structure."
    message = (f"From 0 to 100 rate the stock sentiment around {ticket} and write a short reasoning "
                    f"behind your valuation based on these article:\n{article_body}")
        
    # Call OpenAI API
    res = send_to_open_ai(role=role, message=message, schema=Message)

    result = res.choices[0].message.parsed
    
    # The model gets this wrong sometimes
    result.ticket = ticket

    # Log the result
    print("Recommendation got successflully.")

    return result

