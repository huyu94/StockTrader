from typing import List, Optional

from pydantic import BaseModel


class DataUpdateStatusItem(BaseModel):
    dataset: str
    last_updated_at: str
    coverage_start: Optional[str] = None
    coverage_end: Optional[str] = None
    status: str
    message: Optional[str] = None


class DataUpdateStatusResponse(BaseModel):
    items: List[DataUpdateStatusItem]
