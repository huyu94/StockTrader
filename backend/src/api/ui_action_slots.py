from fastapi import APIRouter

from backend.src.models.ui_action_slot import UiActionSlotResponse
from backend.src.services.ui_action_slot_service import list_action_slots


router = APIRouter()


@router.get("/ui/action-slots", response_model=UiActionSlotResponse)
def get_action_slots() -> UiActionSlotResponse:
    return UiActionSlotResponse(items=list_action_slots())
