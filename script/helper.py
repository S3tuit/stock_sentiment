import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
from script.helper.models import Article
from typing import List


def make_request(url, headers):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def fool_get_news_links(url, headers):

    soup = make_request(url, headers)
    
    # get the section with the 2 articles just below "News & Analysis"
    articles = soup.select('.flex.flex-col')
    
    if not articles:
        return None
    
    # get all the links, the "select" above returns a list
    articles = articles[0].find_all('a', href=True)
    
    links = []
    prefix = r'https://www.fool.com/'
    
    for article in articles:
        links.append(prefix + article['href'])
        
    return links


def fool_get_article_body(soup):    
    article_body = soup.find(class_='article-body')
    
    if not article_body:
        return None
    
    paragraphs = article_body.find_all('p')
    text_content = '\n'.join(paragraph.get_text(strip=True) for paragraph in paragraphs)
    
    return text_content


def extract_date_from_url(url: str) -> datetime:
    # Step 1: Define a regular expression pattern to match the date in the URL
    pattern = r'/(\d{4})/(\d{2})/(\d{2})/'  # Matches /YYYY/MM/DD/
    
    # Step 2: Search for the pattern in the URL
    match = re.search(pattern, url)
    
    if match:
        # Step 3: Extract year, month, and day from the match
        year, month, day = match.groups()
        
        # Step 4: Convert to a datetime object
        date_obj = datetime(int(year), int(month), int(day))
        return date_obj.strftime("%Y-%m-%d")
    else:
        return None


def fool_get_article_info(url, headers, ticket):
    soup = make_request(url, headers)
    
    try:
        text_content = fool_get_article_body(soup)
    except Exception as e:
        text_content = None
        print('Something whent wrong :(. ', e)
        
    try:
        date_time_article = extract_date_from_url(url)
    except Exception as e:
        date_time_article = None
        print('Something whent wrong :(. ', e)
    
    try:
        title = soup.find('h1').text
    except Exception as e:
        title = None
        print('Something whent wrong :(. ', e)
    
    return Article(ticket=ticket, url=url, title=title, article_body=text_content, timestp=date_time_article)


def extract_articles(headers, ticket):
    
    fool_links = []
    # The possible prefix of the URL on fool.com
    prefixes_fool = [r'https://www.fool.com/quote/nasdaq/',
                r'https://www.fool.com/quote/nyse/']

    # get the links
    for prefix in prefixes_fool:
        try:
            url = prefix + ticket + '/'
            links = fool_get_news_links(url, headers)
            
            # if the function finds no link it stops
            if links:
                fool_links = fool_links + links
        
        except Exception as e:
            print('Something whent wrong :(. ', e)


    articles_info: List[Article] = []


    for fool_link in fool_links:
        articles_info.append(fool_get_article_info(url=fool_link, headers=headers, ticket=ticket))
    
    return articles_info