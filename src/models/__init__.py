"""
数据模型模块

使用 Pydantic 定义数据模型，提供：
1. 数据验证
2. 类型检查
3. 数据序列化/反序列化
4. 文档生成
"""

from .stock_models import (
    DailyKlineData,
    BasicInfoData,
    TradeCalendarData,
)

__all__ = [
    "DailyKlineData",
    "BasicInfoData",
    "TradeCalendarData",
]

