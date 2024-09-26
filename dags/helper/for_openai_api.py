
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
        (list): a list containing the result/s.
    '''
    collection = mongo_hook.get_collection(mongo_collection=collection, mongo_db=db)
    result = collection.find({"ticket": ticket}).sort('timestp', -1).limit(limit)
    return list(result)

def get_sentiment(openai_message, format, api_key):
    '''
    Send a message about a stock to openai API. The API analyses the message and retrives the sentiment.
    
    Args:
        openai_message(str): Message to be sent.
        format(class): A class to ensure the results are consistent.
        api_key(str): The openai API key.
        
    Return:
        (class): An object with the same class as format.
    '''
    client = OpenAI(api_key=api_key)
    
    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You're now the best stock analyzer."},
            {"role": "user", "content": openai_message}
        ],
        response_format=format
    )
        
    return completion.choices[0].message.parsed