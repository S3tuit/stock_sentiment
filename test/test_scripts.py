import aiohttp
import asyncio
from bs4 import BeautifulSoup


url = 'https://finance.yahoo.com/quote/ACMR/news/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
}
cookies = {
    'GUC': 'AQABCAFmwcNm80IfWgSU&s=AQAAAGoqDQ7v&g=ZsB0FA',
    'A1': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'A3': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'A1S': 'd=AQABBBbSSWYCEMGNccbgJJ6fo37cGXpNRK4FEgABCAHDwWbzZudVb2UBAiAAAAcIFNJJZkCw6_E&S=AQAAAhcGLnTIGaPvJkY30Nu1ux4',
    'PRF': 't%3DACMR%26newChartbetateaser%3D0%252C1725284010825'
}



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

async def fetch_article_content(session, link):
    article_content = await fetch_page(session, link)
    if article_content:
        article_soup = BeautifulSoup(article_content, 'html.parser')
        body = article_soup.find(class_="caas-body")
        if body:
            return "\n".join(p.get_text(strip=False) for p in body.find_all('p'))
    return None


async def fetch_yahoo():
    async with aiohttp.ClientSession(headers=HEADERS, cookies=cookies) as session:
        # Fetch the main page
        page_content = await fetch_page(session, url)
        if not page_content:
            return

        soup = BeautifulSoup(page_content, 'html.parser')

        articles = []
        tasks = []

        # Process news articles
        for news in soup.find_all(class_='js-stream-content Pos(r)'):
            article = news.find('h3').find('a')
            title = article.get_text(strip=True)
            link = article.get('href')

            if 'ACMR' in title:
                tasks.append(fetch_article_content(session, link))

        # Gather results
        results = await asyncio.gather(*tasks)
        articles = [result for result in results if result]
        
asyncio.run(fetch_yahoo())
print(articles)