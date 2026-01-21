from typing import List

from backend.src.models.ui_action_slot import UiActionSlotItem


def list_action_slots() -> List[UiActionSlotItem]:
    """
    获取预留采集按钮入口列表
    
    返回预定义的UI操作按钮配置，用于在界面上展示预留的数据采集入口。
    当前所有按钮状态为 disabled，表示功能尚未启用。
    
    Returns:
        预留按钮配置列表
    """
    return [
        UiActionSlotItem(
            id="fetch_daily_kline",
            label="拉取日线",
            state="disabled",
            tooltip="功能未启用",
        ),
        UiActionSlotItem(
            id="fetch_basic_info",
            label="同步基础信息",
            state="disabled",
            tooltip="功能未启用",
        ),
        UiActionSlotItem(
            id="fetch_calendar",
            label="同步交易日历",
            state="disabled",
            tooltip="功能未启用",
        ),
    ]
