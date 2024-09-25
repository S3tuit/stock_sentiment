
from openai import OpenAI


def get_info_from_mongo(mongo_hook, collection, ticket, db='stock_test', limit=1):
    '''
    Retrives data from Mongo based on the query {"ticket": ticket} and sorted by timestp descending.
    
    Args:
        mongo_hook(MongoHook): Hook from Airflow.
        collection(str): Mongo collection.
        ticket(str): stock ticket.
        db(str): Mogno db.
        limit(int): How many documents to retrieve.
        
    Return:
        (pymongo.cursor.Cursor): use it into a list() to get the result/s.
    '''
    collection = mongo_hook.get_collection(mongo_collection=collection, mongo_db=db)
    return collection.find({"ticket": ticket}).sort('timestp', -1).limit(limit)

def get_sentiment(openai_message, format):
    '''
    Send a message about a stock to openai API. The API analyses the message and retrives the sentiment.
    
    Args:
        openai_message(str): Message to be sent.
        format(class): A class to ensure the results are consistent.
        
    Return:
        (class): An object with the same class as format.
    '''
    client = OpenAI()
    
    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You're now the best stock analyzer."},
            {"role": "user", "content": openai_message}
        ],
        response_format=format
    )
        
    return completion.choices[0].message.parsed