"""
数据加载层模块

提供所有数据加载器的基类和具体实现
"""

from core.loaders.base import BaseLoader
from core.loaders.daily_kline import DailyKlineLoader
from core.loaders.adj_factor import AdjFactorLoader
from core.loaders.basic_info import BasicInfoLoader
from core.loaders.trade_calendar import TradeCalendarLoader

__all__ = [
    "BaseLoader",
    "DailyKlineLoader",
    "AdjFactorLoader",
    "BasicInfoLoader",
    "TradeCalendarLoader",
]

