from typing import Optional

from fastapi import APIRouter, Query

from backend.src.models.daily_kline import DailyKlineResponse
from backend.src.services.daily_kline_service import fetch_daily_klines


router = APIRouter()


@router.get("/stocks/{ts_code}/daily", response_model=DailyKlineResponse)
def get_daily_kline(
    ts_code: str,
    start_date: Optional[str] = Query(default=None, description="YYYYMMDD or YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYYMMDD or YYYY-MM-DD"),
) -> DailyKlineResponse:
    items = fetch_daily_klines(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return DailyKlineResponse(ts_code=ts_code, items=items)
