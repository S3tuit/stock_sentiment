import aiohttp
import asyncio
from bs4 import BeautifulSoup
from helper.models import Article
from typing import List
from datetime import datetime

# Gets the text content of the page
async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()
        
    except aiohttp.ClientError as e:
        print(f"Request failed: {e}")
        return None
    
    except Exception as e:
        print(f"Something went wrong while fetching {url}: {e}")
        return None


async def fetch_yahoo_article_content(session, link, title, ticker):
    article_content = await fetch_page(session, link)
    print(f'Page fetched for the link {link}.')
    
    if article_content:
        article_soup = BeautifulSoup(article_content, 'html.parser')
        body = article_soup.find(class_="caas-body") # This find the body of the article
        
        if body:
            print('Article body found!')
            # This return the concatenation of all the text inside the paragraphs of the article
            article_content = "\n".join(p.get_text(strip=False) for p in body.find_all('p'))
            return Article(
                ticker=ticker,
                timestp=int(datetime.now().timestamp()),
                url=link,
                title=title,
                article_body=article_content
            )
        
    print('No article body found.')
    return None



# This return a list of tasks to run async
async def fetch_yahoo(ticker, headers, cookies):
    
    ticker = ticker.upper()
    url = f'https://finance.yahoo.com/quote/{ticker}/news/'
    async with aiohttp.ClientSession(headers=headers, cookies=cookies) as session:
        
        # Fetch the main page
        page_content = await fetch_page(session, url)
        print(f'Page fetched for the link {url}.')
        if not page_content:
            print('No page content found.')
            return

        soup = BeautifulSoup(page_content, 'html.parser')

        # Instanciate a list of Article entities
        articles: List[Article] = []
        tasks = []

        # This locate the section on the main page with all the article links
        for news in soup.find_all(class_='js-stream-content Pos(r)'):
            
            article = news.find('h3').find('a')
            title = article.get_text(strip=True)
            link = article.get('href')
            
            # Check if the url is about an add and not about the stock
            if ticker in title:
                tasks.append(fetch_yahoo_article_content(session, link, title, ticker))

        # Process the tasks as they complete
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                articles.append(result)
        
        return articles