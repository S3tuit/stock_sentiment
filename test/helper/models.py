from pydantic import BaseModel
from typing import Optional

# Class used for the topic test.articles
class Article(BaseModel):
    ticker: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    article_body: Optional[str] = None
    timestp: Optional[int] = None

# Class used for the topic test.price_info
class Prices(BaseModel):
    ticker: str
    timestp: int    # in unix time
    price_n_volume: dict    # daily price and volume
    technicals: dict    # technical analysis, {indicator: value}

class BalanceSheet(BaseModel):
    ticker: str
    timestp: int    # in unix time
    earnings_ratios: dict    # earnings and ratios relative to them (like PS)
    balance_sheet: dict
