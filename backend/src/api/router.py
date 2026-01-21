from fastapi import APIRouter

from backend.src.api.health import router as health_router
from backend.src.api.daily_kline import router as daily_kline_router
from backend.src.api.stocks import router as stocks_router
from backend.src.api.ui_action_slots import router as action_slots_router
from backend.src.api.data_update_status import router as data_update_status_router


api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(stocks_router, tags=["stocks"])
api_router.include_router(daily_kline_router, tags=["daily_kline"])
api_router.include_router(action_slots_router, tags=["action_slots"])
api_router.include_router(data_update_status_router, tags=["data_status"])
