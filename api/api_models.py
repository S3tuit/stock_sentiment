from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Recommendation(BaseModel):
    ticket: Optional[str] = None
    valutation: Optional[int] = None
    reasoning: Optional[int] = None

class Message(BaseModel):
    ticket: str
    sentiment: int
    message: str
    

# Define Pydantic model for the request
class TicketRequest(BaseModel):
    ticket: str