from datetime import datetime, timedelta
import logging
from bs4 import BeautifulSoup
import requests

from airflow.decorators import dag, task
from airflow.models import Variable

from helper.models import Article
from helper.kafka_produce import make_producer, ProducerCallback
from helper import schemas

# Constants for Kafka and API configurations
TOPIC_NAME = "test.articles"
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Constants for scraping
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127.0.0.0'
}
COOKIES = {
    'GUC': 'AQABCAFmwcNm80IfWgSU&s=AQAAAGoqDQ7v&g=ZsB0FA',
    'A1': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'A3': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'A1S': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'PRF': 't%3DACMR%26newChartbetateaser%3D0%252C1725284010825'
}

tickets = ['ACMR', 'RIOT', 'HAL']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def yahoo_extract():
    """
    DAG to extract article data from the Yahoo News, process the data,
    and produce it to a Kafka topic.
    """

    @task(
        task_id='get_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def get_news_links_task(tickets):
        """
        Task to retrieve news links for specific stocks from Yahoo News.
        
        Args:
            tickets (list): The stock ticker symbols.
        
        Returns:
            list: A list of dictionaries containing article metadata.
        """
        raw_articles = []
        
        for ticket in tickets:
            url = f'https://finance.yahoo.com/quote/{ticket}/news/'
            
            # Fetch the main page using requests
            response = requests.get(url, headers=HEADERS, cookies=COOKIES)
            
            if response.status_code != 200:
                logger.error(f'Failed to retrieve content for ticket {ticket}. Status code: {response.status_code}')
                response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Locate the section on the main page with all the article links
            for news in soup.find_all(class_='js-stream-content Pos(r)'):
                
                article = news.find('h3').find('a')
                title = article.get_text(strip=True)
                link = article.get('href')
                
                # Check if the URL is about the stock and is not an ad
                if ticket in title:
                    raw_articles.append({
                        'ticket': ticket,
                        'link': link,
                        'title': title
                    })
                    
                    logger.info(f"Successfully retrieved an article for ticker {ticket}.")
                    
        logger.info(f"Total articles found: {len(raw_articles)}.")
        return raw_articles


    @task(
        task_id='process_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=30)
    )
    def process_links_task(raw_articles):
        """
        Task to process raw article data, extract content, and produce it to a Kafka topic.
        
        Args:
            raw_articles (list): List of dictionaries containing raw article metadata.
        """
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.article_schema_v1
        )
        logger.info("Kafka producer created successfully.")
        
        for raw_article in raw_articles:
            response = requests.get(raw_article['link'], headers=HEADERS, cookies=COOKIES)
            logger.info(f"Page fetched for the link {raw_article['link']}.")
            
            if response.status_code == 200:
                article_soup = BeautifulSoup(response.text, 'html.parser')
                body = article_soup.find(class_="caas-body")  # This finds the body of the article
                
                if body:
                    logger.info(f"Article body found for the link {raw_article['link']}.")
                    
                    # This returns the concatenation of all the text inside the paragraphs of the article
                    article_content = "\n".join(p.get_text(strip=False) for p in body.find_all('p'))
                    article = Article(
                        ticket=raw_article['ticket'],
                        timestp=int(datetime.now().timestamp()),
                        url=raw_article['link'],
                        title=raw_article['title'],
                        article_body=article_content
                    )
                    
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=article.ticket.lower(),
                        value=article,
                        on_delivery=ProducerCallback(article)
                    )
                
                else:
                    logger.warning(f"Article body NOT found for the link {raw_article['link']}.")
            else:
                logger.error(f"Failed to fetch the article content for link {raw_article['link']}. Status code: {response.status_code}")
                response.raise_for_status()

        producer.flush()
        logger.info("Kafka producer flushed successfully.")

    articles = get_news_links_task(tickets)
    process_links_task(articles)

yahoo_extract()
