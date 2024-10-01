


def get_latest_articles(client, source):

    db = client.stock_test
    articles_collection = db.articles_test

    # MongoDB aggregation pipeline
    pipeline = [
        {"$match": {"source": source}},  # Match the source
        {"$sort": {"timestamp": -1}},  # Sort by timestamp in descending order (latest first)
        {"$group": {
            "_id": "$ticket",  # Group by ticket
            "ticket": {"$first": "$ticket"},
            "title": {"$first": "$title"},  # Get the title of the latest article
            "source": {"$first": "$source"}
        }},
        {"$project": {"_id": 0, "ticket": 1, "title": 1, "source": 1}}  # Exclude _id field
    ]

    latest_articles = list(articles_collection.aggregate(pipeline))
    
    return latest_articles


def upsert_articles(article_entities, client):
    # Perform upsert operations on articles_cache
    db = client.stock_test
    cache_collection = db.articles_cache
    
    for article in article_entities:
        cache_collection.update_one(
            {"ticket": article["ticket"], "source": article["source"]},  # Match criteria
            {"$set": {
                "title": article["title"]
            }},
            upsert=True  # Upsert flag to insert if no match is found
        )

def get_cache(client, source):
        db = client.stock_test
        collection = db.articles_cache

        # Query to find documents with ticket "RIOT" and limit to the 3 most recent
        mongo_result = collection.find({"source": source}, {"ticket": 1, "title": 1, '_id': 0})
        
        cached_articles = {article['ticket']: article['title'] for article in mongo_result}
        
        return cached_articles        