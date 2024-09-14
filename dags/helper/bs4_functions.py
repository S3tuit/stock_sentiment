import requests
from bs4 import BeautifulSoup


def get_soup(logger, ticket, urls, **kwargs):
    """
    Fetch the content of a URL and parse it with BeautifulSoup.
        
    Args:
        ticket (str): The stock ticker symbol.
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
            logger.error(f'Failed to retrieve content for ticket {ticket}, url {url}. Status code: {response.status_code}. Error: {http_err}')
            # raise
        except Exception as err:
            logger.error(f'An error occurred for ticket {ticket}, url {url}. Error: {err}')
            # raise
    
    if success == False:
        raise
    
    logger.info(f'Page found for url: {url}')
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup