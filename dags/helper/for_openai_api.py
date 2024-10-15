
from openai import OpenAI
from .models import StockSentiment
import re
from time import sleep
from datetime import datetime, timedelta


def get_info_from_mongo(mongo_hook, collection, ticker, db='stock_test', limit=1):
    '''
    Retrives data from Mongo based on the query {"ticker": ticker} and sorted by timestp descending.
    
    Args:
        mongo_hook(MongoHook): Hook from Airflow.
        collection(str): Mongo collection.
        ticker(str): stock ticker.
        db(str): Mogno db.
        limit(int): How many documents to retrieve.
        
    Return:
        (list): a list containing the result/s.
    '''
    collection = mongo_hook.get_collection(mongo_collection=collection, mongo_db=db)
    result = collection.find({"ticker": ticker}).sort('timestp', -1).limit(limit)
    return list(result)


def get_sentiment(openai_message, api_key, ticker, logger, max_retries=3, retry_delay=2):
    '''
    Send a message about a stock to the OpenAI API. The API analyzes the message and retrieves the sentiment.
    This version of openai doesn't provide a structured output, this is a workaround.

    Args:
        openai_message(str): The message about the stock for analysis.
        api_key(str): API key to authenticate with OpenAI.
        ticker(str): Stock ticker symbol.
        logger(logging.Logger): Logger for tracking retries, warnings, and errors.
        max_retries(int): Maximum number of retries if the response format is incorrect (default is 3).
        retry_delay(int): Delay (in seconds) between retries (default is 2 seconds).

    Returns:
        (StockSentiment): A StockSentiment object containing next_month_prediction, next_year_prediction, and reasoning.

    Raises:
        ValueError: If maximum retries are reached without a valid response from OpenAI.
    '''

    client = OpenAI(api_key=api_key)
    system_role = '''You're now the best stock analyzer.
    Respond using this format: Next Month Prediction: your_prediction, Next Year Prediction: your_prediction, Reasoning: your_reasoning.'''
    
    retries = 0
    
    while retries < max_retries:
        try:
            
            response_openai = 'None'
            # Make API request
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": openai_message}
                ]
            )
            
            response_openai = completion.choices[0].message.content
            
            # Regular expressions to capture the desired values:
            # Match all the number and . after word: whitespaces, 0 or more "*$.
            next_month = re.search(r'Next Month Prediction:\s*["*$]*([\d.]+)', response_openai, re.IGNORECASE).group(1)
            next_year = re.search(r'Next Year Prediction:\s*["*$]*([\d.]+)', response_openai, re.IGNORECASE).group(1)
            reasoning = re.search(r'Reasoning:\s*[*"#]*\s*(.*)', response_openai, re.IGNORECASE).group(1)

            next_month_prediction = float(next_month)
            next_year_prediction = float(next_year)
            
            # Get today's date at midnight
            midnight_today = datetime.combine(datetime.today(), datetime.min.time())
                
            # Create the sentiment object
            sentiment = StockSentiment(
                next_month_prediction=next_month_prediction,
                next_year_prediction=next_year_prediction,
                reasoning=reasoning,
                ticker=ticker.lower(),
                timestp=int(midnight_today.timestamp())
            )
            return sentiment

        except Exception as e:
            # Log the exception and retry attempt
            logger.warning(f"Attempt {retries + 1} failed: {str(e)}")
            logger.warning(f"Here was the response from openai API: {response_openai}")
        
        finally:
            # Increment retry count and sleep before retrying
            retries += 1
            sleep(retry_delay)

    # If max retries exceeded, log an error and raise an exception
    logger.error(f"Max retries exceeded. Unable to get a valid response from OpenAI for ticker: {ticker}.")
    raise ValueError(f"Failed to retrieve a valid sentiment after {max_retries} attempts.")
