"""
存储模块初始化
使用MySQL存储（通过SQLAlchemy ORM）
"""


# 使用MySQL存储
from .basic_info_storage_mysql import BasicInfoStorageMySQL as BasicInfoStorage
from .calendar_storage_mysql import CalendarStorageMySQL as CalendarStorage
from .daily_kline_storage_mysql import DailyKlineStorageMySQL as DailyKlineStorage
from .mysql_base import MySQLBaseStorage as BaseStorage
# ORM 模型已迁移到 core.models.orm，这里重新导出以保持向后兼容
from core.models.orm import *

__all__ = [
    "BasicInfoStorage",
    "CalendarStorage",
    "DailyKlineStorage",
    "BaseStorage",
    "BasicInfoStorageMySQL",
    "CalendarStorageMySQL",
    "DailyKlineStorageMySQL",
    "MySQLBaseStorage",
]

# 导出所有存储类（包括具体实现）
from .basic_info_storage_mysql import BasicInfoStorageMySQL
from .calendar_storage_mysql import CalendarStorageMySQL
from .daily_kline_storage_mysql import DailyKlineStorageMySQL
from .mysql_base import MySQLBaseStorage
