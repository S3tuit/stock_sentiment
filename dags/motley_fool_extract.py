from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models import Variable

from helper.models import Article
from helper.kafka_produce import make_producer, ProducerCallback
from helper import schemas
from helper.bs4_functions import get_soup

# Constants for Kafka and API configurations
TOPIC_NAME = "test.articles"
SCHEMA_REGISTRY_URL = Variable.get("SCHEMA_REGISTRY_URL")
BOOTSTRAP_SERVERS = Variable.get("BOOTSTRAP_SERVERS")

# Constants for scraping
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127.0.0.0'
}

tickets = ['ACMR', 'RIOT']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=["stock_sentiment"]
)
def motley_fool_extract():
    """
    DAG to extract article data from Motley Fool, process the data,
    and produce it to a Kafka topic.
    """

    @task(
        task_id='get_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5)
    )
    def get_news_links_task(tickets):
        """
        Task to retrieve news links for specific stocks from Motley Fool.
        
        Args:
            tickets (list): The stock ticker symbols.
        
        Returns:
            list: A list of dictionaries containing article metadata; url, title, ticket.
        """
        article_basic_info = []
        
        for ticket in tickets:
            url = f'https://www.fool.com/quote/nasdaq/{ticket}/'
            
            soup = get_soup(logger=logger, ticket=ticket, urls=[url], headers=HEADERS)
            
            # Locate the section on the main page with the article links
            news_div = soup.find(id="quote-news-analysis")
            
            # get the title, link and ticket of the first 2 articles
            article_basic_info += [{'url': a.get('href'), 'title': a.get('data-track-link'), 'ticket': ticket} for a in news_div.find_all('a')[:2]]
                
            # There's no check to see if the ticket is in the title because fool.com doesn't show ads in that div.
                    
            logger.info(f"Successfully retrieved article link for ticket: {ticket}.")
                    
        logger.info(f"Total articles found: {len(article_basic_info)}.")
        return article_basic_info


    @task(
        task_id='process_the_links',
        retries=0,
        retry_delay=timedelta(seconds=5),
        execution_timeout=timedelta(seconds=300)
    )
    def process_links_task(article_basic_info):
        """
        Task to process raw article data, extract content, and produce it to a Kafka topic.
        
        Args:
            raw_articles (list): List of dictionaries containing raw article metadata; link, title, ticket.
        """
        
        if not article_basic_info:
            logger.warning("No articles to process.")
            return
        
        producer = make_producer(
            schema_reg_url=SCHEMA_REGISTRY_URL,
            bootstrap_server=BOOTSTRAP_SERVERS,
            schema=schemas.article_schema_v1
        )
        logger.info("Kafka producer created successfully.")
        
        for basic_article in article_basic_info:
            
            url = 'https://www.fool.com/' + basic_article['url']
            logger.info(f'Making request for: {url}')
            soup = get_soup(logger=logger, ticket=basic_article['ticket'], urls=[url], headers=HEADERS)
            logger.info(f'Request made for: {url}')
            
            article_body = soup.select('div.article-body')
            
            if article_body:
                article_body = article_body[0]
                article_text = '\n'.join(paragraph.get_text() for paragraph in article_body.find_all('p'))
                
                if article_text:
                    article = Article(
                        ticket=basic_article['ticket'],
                        url=basic_article['url'],
                        title= basic_article['title'],
                        article_body= article_text,
                        timestp=int(datetime.now().timestamp())
                    )
                    
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=article.ticket.lower(),
                        value=article,
                        on_delivery=ProducerCallback(article)
                    )
                
                else:
                    logger.warning(f"No text found for the article: {basic_article['url']} -- ticket: {basic_article['ticket']}.")
            
            else:
                logger.warning(f"No article body found for the ticket: {basic_article['ticket']} -- url: {basic_article['url']}")

        producer.flush()
        logger.info("Kafka producer flushed successfully.")



    articles = get_news_links_task(tickets)
    process_links_task(articles)

motley_fool_extract()
