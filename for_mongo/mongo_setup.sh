#!/bin/bash

# Define the connection string
MONGO_URI="mongodb://peppa:peppa@mongodb:27017"

# Connect to MongoDB and execute commands
docker exec -it mongosh mongosh "$MONGO_URI" <<EOF
use stock_test;


db.articles_test.createIndex(
   { ticker: 1, url: 1 },
   { unique: true, name: "articles_test_uniq_ticket_url" }
);
db.articles_test.createIndex(
    { timestp: -1 },
    { name: "articles_test_timestp_index"}
);
db.articles_test.createIndex(
    { source: 1 },
    { name: "articles_test_source_index"}
);


db.balance_sheet.createIndex(
   { ticker: 1, timestp: -1 },
   { unique: true, name: "balance_sheet_test_ticket_tmstp" }
);


db.price_info.createIndex(
   { ticker: 1, timestp: -1 },
   { unique: true, name: "price_test_ticket_tmstp" }
);


db.stock_sentiment.createIndex(
   { ticker: 1, timestp: -1 },
   { unique: true, name: "sentiment_test_ticket_tmstp" }
);


db.articles_cache.createIndex(
  { "ticker": 1, "source": 1 },
  { unique: true, name: "articles_cache_ticket_source" }
);
db.articles_cache.createIndex(
    { source: 1 },
    { name: "articles_cache_source_index"}
);


EOF

echo "MongoDB setup complete: Indexes created."
