import requests
from bs4 import BeautifulSoup
from time import sleep
from random import choice


def get_soup(logger, ticker, urls, **kwargs):
    """
    Fetch the content of a URL and parse it with BeautifulSoup.
        
    Args:
        ticker (str): The stock ticker symbol.
        logger (logging.Logger): An instance of the logging library.
        urls (list): a list of url to test.
        kwargs: Additional arguments to be used for requests.get().
        
    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML.
        
    """

    success = False
    for url in urls:
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx and 5xx)
            success = True
            break
        except requests.HTTPError as http_err:
            logger.error(f'Failed to retrieve content for ticker {ticker}, url {url}. Status code: {response.status_code}. Error: {http_err}')
            # raise
        except Exception as err:
            logger.error(f'An error occurred for ticker {ticker}, url {url}. Error: {err}')
            # raise
    
    if success == False:
        raise
    
    logger.info(f'Page found for url: {url}')
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def wait_and_rotate_agent(wait_time=10):
    """
    Sleep for the specified time and return a randomly chosen user-agent string.
    This helps avoid detection and rate-limiting when scraping.
    
    Args:
        wait_time (int): The time in seconds to wait before proceeding.
    
    Returns:
        dict: A dictionary with the 'User-Agent' header.
    """
    
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127.0.0.0'
    ]
    
    sleep(wait_time)
    return {'User-Agent': choice(user_agents)}