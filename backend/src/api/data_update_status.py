from fastapi import APIRouter

from backend.src.models.data_update_status import DataUpdateStatusResponse
from backend.src.services.data_update_status_service import fetch_data_update_status


router = APIRouter()


@router.get("/status/data-updates", response_model=DataUpdateStatusResponse)
def get_data_update_status() -> DataUpdateStatusResponse:
    return DataUpdateStatusResponse(items=fetch_data_update_status())
