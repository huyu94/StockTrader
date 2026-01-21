from typing import List

from pydantic import BaseModel


class UiActionSlotItem(BaseModel):
    id: str
    label: str
    state: str
    tooltip: str


class UiActionSlotResponse(BaseModel):
    items: List[UiActionSlotItem]
