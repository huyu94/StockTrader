from typing import List, Optional

from pydantic import BaseModel


class StockItem(BaseModel):
    ts_code: str
    name: str
    market: Optional[str] = None


class StockListResponse(BaseModel):
    items: List[StockItem]
    total: int
