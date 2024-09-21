#!/bin/bash

# Define the connection string
MONGO_URI="mongodb://peppa:peppa@mongodb:27017"

# Connect to MongoDB and execute commands
docker exec -it mongosh mongosh "$MONGO_URI" <<EOF
use stock_test;

# Create indexes for articles_test
db.articles_test.createIndex(
   { ticket: 1, url: 1 },
   { unique: true, name: "articles_test_uniq_ticket_url" }
);
db.articles_test.createIndex(
    { timestp: -1 },
    { name: "articles_test_timestp_index" }
);


# Create indexes for balance_sheet
db.balance_sheet.createIndex(
   { ticket: 1, timestp: -1 },
   { unique: true, name: "balance_sheet_test_ticket_tmstp" }
);


# Create indexes for price_info
db.price_info.createIndex(
   { ticket: 1, timestp: -1 },
   { unique: true, name: "price_test_ticket_tmstp" }
);


EOF

echo "MongoDB setup complete: Indexes created."
