from typing import List, Optional

from pydantic import BaseModel


class DailyKlineItem(BaseModel):
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: Optional[float] = None
    adj_factor: Optional[float] = None


class DailyKlineResponse(BaseModel):
    ts_code: str
    items: List[DailyKlineItem]
