from pydantic import BaseModel
from typing import Optional

# Class used for the topic test.articles
class Article(BaseModel):
    ticket: str
    url: str
    title: str
    article_body: str
    timestp: int
    source: str

# Class used for the topic test.price_info
class Prices(BaseModel):
    ticket: str
    timestp: int    # in unix time
    price_n_volume: dict    # daily price and volume
    technicals: dict    # technical analysis, {indicator: value}

class BalanceSheet(BaseModel):
    ticket: str
    timestp: int    # in unix time
    earnings_ratios: dict    # earnings and ratios relative to them (like PS)
    balance_sheet: dict

# Used to retrieve structured result with openai API
class StockSentiment(BaseModel):
    next_month_prediction: float
    next_year_prediction: float
    reasoning: str
    ticket: str
    timestp: int