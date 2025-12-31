"""
数据采集层模块

提供所有数据采集器的基类和具体实现
"""

from core.collectors.base import BaseCollector
from core.collectors.daily_kline import DailyKlineCollector
from core.collectors.adj_factor import AdjFactorCollector
from core.collectors.ex_date import ExDateCollector
from core.collectors.basic_info import BasicInfoCollector
from core.collectors.trade_calendar import TradeCalendarCollector

__all__ = [
    "BaseCollector",
    "DailyKlineCollector",
    "AdjFactorCollector",
    "ExDateCollector",
    "BasicInfoCollector",
    "TradeCalendarCollector",
]

