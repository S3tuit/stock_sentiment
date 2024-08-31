from openai import OpenAI
import os
from pymongo import MongoClient
import urllib.parse

# MongoDB credentials
username = urllib.parse.quote_plus('peppa')
password = urllib.parse.quote_plus('peppa')

# Construct the MongoDB URI
uri = f"mongodb://{username}:{password}@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"

# Create a MongoClient
client = MongoClient(uri)

db = client.stock_test.articles_test


def get_article(ticket):
    # MongoDB credentials
    username = urllib.parse.quote_plus('peppa')
    password = urllib.parse.quote_plus('peppa')

    # Construct the MongoDB URI
    uri = f"mongodb://{username}:{password}@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"

    # Create a MongoClient
    client = MongoClient(uri)
    
    db = client.stock_test.articles_test
    
    article = db.find_one(
        {'ticket': ticket},
        sort=[('timestp', -1)]  # Sort by timestp in descending order
    )
    
    return article

def send_to_open_ai(role, message, schema):
    client = OpenAI(api_key=os.environ['OPENAI_KEY'])

    response = client.beta.chat.completions.parse(
    model="gpt-4o-mini-2024-07-18",
    messages=[
        {"role": "system", "content": role},
        {"role": "user", "content": message}
    ],
    response_format=schema
    )
    
    return response