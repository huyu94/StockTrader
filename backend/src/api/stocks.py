from typing import Optional

from fastapi import APIRouter, Query

from backend.src.models.stock import StockListResponse
from backend.src.services.stock_service import search_stocks


router = APIRouter()


@router.get("/stocks", response_model=StockListResponse)
def list_stocks(
    query: Optional[str] = Query(default=None, description="股票代码或名称关键字"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> StockListResponse:
    items, total = search_stocks(query=query, limit=limit, offset=offset)
    return StockListResponse(items=items, total=total)
