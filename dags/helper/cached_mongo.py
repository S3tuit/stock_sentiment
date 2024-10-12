

### ARTICLES

def get_latest_articles(client, source):
    '''
    Retrieve the latest articles from a MongoDB collection for a specific source.

    Args:
        client (MongoClient): The MongoDB client used to connect to the database.
        source (str): The source of the articles to be retrieved (e.g., "seeking_alpha", "motley_fool").

    Returns:
        list[dict]: A list of dictionaries representing the latest articles for each stock.
                    Each dictionary contains: "ticker", "title", "source"
    '''

    db = client.stock_test
    articles_collection = db.articles_test

    # MongoDB aggregation pipeline
    pipeline = [
        {"$match": {"source": source}},  # Match the source
        {"$sort": {"timestamp": -1}},  # Sort by timestamp in descending order (latest first)
        {"$group": {
            "_id": "$ticker",  # Group by ticker
            "ticker": {"$first": "$ticker"},
            "title": {"$first": "$title"},  # Get the title of the latest article
            "source": {"$first": "$source"}
        }},
        {"$project": {"_id": 0, "ticker": 1, "title": 1, "source": 1}}  # Exclude _id field
    ]

    latest_articles = list(articles_collection.aggregate(pipeline))
    
    return latest_articles


def upsert_articles(article_entities, client):
    '''
    Perform an upsert operation for a list of articles into the MongoDB cache.

    Args:
        article_entities (list[dict]): A list of dictionaries representing articles.
                                        Each dictionary should contain: "ticker", "title", "source"
        client (MongoClient): The MongoDB client used to connect to the database.
    
    Returns:
        None: This function does not return anything but updates the MongoDB collection in place.

    '''
    
    # Perform upsert operations on articles_cache
    db = client.stock_test
    cache_collection = db.articles_cache
    
    for article in article_entities:
        cache_collection.update_one(
            {"ticker": article["ticker"], "source": article["source"]},  # Match criteria
            {"$set": {
                "title": article["title"]
            }},
            upsert=True  # Upsert flag to insert if no match is found
        )

def get_cached_articles(client, source):
    '''
    Retrieve all cached articles for a specific source from the MongoDB cache.

    Args:
        client (MongoClient): The MongoDB client used to connect to the database.
        source (str): The source of the articles to retrieve from the cache (e.g., "seeking_alpha", "motley_fool").

    Returns:
        dict: A dictionary where the keys are stock tickers (str), and the values are article titles (str).
    '''
    
    db = client.stock_test
    collection = db.articles_cache

    mongo_result = collection.find({"source": source}, {"ticker": 1, "title": 1, '_id': 0})
        
    cached_articles = {article['ticker']: article['title'] for article in mongo_result}
        
    return cached_articles        


### BALANCE SHEET
def get_latest_balance_time(client, tickers, timestp_threshold):
    '''
    Retrieve the latest articles from a MongoDB collection for specific tickers,
    where the latest article's `timestp` is less than the given threshold.

    Args:
        client (MongoClient): The MongoDB client used to connect to the database.
        tickers (list): The stock tickers to retrieve articles for.
        timestp_threshold (int): The maximum `timestp` value for the latest article. Unix time.

    Returns:
        list: A list that contains all the tickers that have the latest balance sheet before timestp_threshold
    '''

    db = client.stock_test
    balance_sheet_collection = db.balance_sheet

    pipeline = [
        {"$match": {"ticker": {"$in": tickers}}},
        {"$sort": {"ticker": 1, "timestp": -1}},  # Sort by ticker and then by timestamp (most recent first)
        {"$group": {
            "_id": "$ticker",
            "latest_timestp": {"$first": "$timestp"} # Get the timestp of the latest balance sheet
        }},
        {"$match": {"latest_timestp": {"$lt": timestp_threshold}}}, # Retrieve the timestp only if it's lower than timestp_threshold
        {"$project": {
            "_id": 0,
            "ticker": "$_id",
            "timestp": "$latest_timestp"
        }}
    ]

    latest_balances = list(balance_sheet_collection.aggregate(pipeline))
    
    too_old_balance_sheet = [stock['ticker'] for stock in latest_balances]
    
    return too_old_balance_sheet