# Correlation Between Stock Sentiment and Stock Price

This system extracts stock-related articles from various online sources... analyzes their sentiment and price estimates using OpenAI's API... and stores everything in a database.

The goal is to see whether there is any correlation between the sentiment derived from stock articles and the future price movements of the related stocks.

## System Setup

### Docker

To build and start the system using Docker, run the following commands:

```bash
docker-compose build
docker-compose up -d
 ```

## Kafka

After starting the system, make an API call to Kafka Connect to create the necessary connectors. You can do this by sending a POST request to the following URL:

 ```
http://localhost:8083/connectors
 ```

For each JSON file in the `for_kafka` directory, use it as the body of the POST request. These files define the connectors.

The following topics need to be created for the system to function correctly: 
- `test.articles_v2`
- `test.balance_sheet`
- `test.openai_sentiment`
- `test.price_info`

## MongoDB

To set up MongoDB, run the following script (make sure to replace `MONGO_URI` with your own credentials):

 ```bash
for_mongo/mongo_setup.sh
 ```

## Airflow

You need to import the following variables and replace them with your specific credentials:

- `SEEK_ALPHA_API_KEY`: Your Seeking Alpha API key,
- `SEEK_ALPHA_API_HOST`: "seeking-alpha.p.rapidapi.com",
- `SCHEMA_REGISTRY_URL`: "http://schema-registry:8081",
- `BOOTSTRAP_SERVERS`: "broker:29092",
- `TELEGRAM_CHAT`: Your Telegram chat ID,
- `ALPHA_VANTAGE_API_KEY`: Alpha Vantage API key,
- `OPENAI_API_KEY`: OpenAI secret API key.

## Data Persistence

This Docker setup uses volumes, ensuring that the following data is preserved:

- Airflow database
- Kafka topics
- Kafka Connect connectors
- MongoDB data
