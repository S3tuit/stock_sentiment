import requests
from bs4 import BeautifulSoup


def get_soup(logger, ticket, url, **kwargs):
    """
    Fetch the content of a URL and parse it with BeautifulSoup.
        
    Args:
        ticket (str): The stock ticker symbol.
        logger (logging.Logger): An instance of the logging library.
        url (str): the url of the page.
        kwargs: Additional arguments to be used for requests.get().
        
    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML.
        
    Raises:
        ValueError: If 'url' is not provided in kwargs.
    """

    try:
        response = requests.get(url, **kwargs)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx and 5xx)
    except requests.HTTPError as http_err:
        logger.error(f'Failed to retrieve content for ticket {ticket}. Status code: {response.status_code}. Error: {http_err}')
        raise
    except Exception as err:
        logger.error(f'An error occurred for ticket {ticket}. Error: {err}')
        raise
    
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup