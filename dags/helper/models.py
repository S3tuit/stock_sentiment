from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Article(BaseModel):
    ticket: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    article_body: Optional[str] = None
    timestp: Optional[int] = None
