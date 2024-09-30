


def get_cache(client, source):

        db = client.stock_test
        collection = db.articles_cache

        # Query to find documents with ticket "RIOT" and limit to the 3 most recent
        mongo_result = collection.find({"source": source}, {"ticket": 1, "title": 1, '_id': 0})
        
        cached_articles = {article['ticket']: article['title'] for article in mongo_result}
        
        return cached_articles