"""
SQLAlchemy ORM 模型定义（向后兼容层）

注意：ORM 模型已迁移到 core.models.orm
此文件保留用于向后兼容，实际模型定义在 core.models.orm 中
"""

# 从新位置重新导出所有 ORM 模型，保持向后兼容
from core.models.orm import (
    Base,
    DailyKlineORM,
    AdjFactorORM,
    BasicInfoORM,
    TradeCalendarORM,
)

__all__ = [
    "Base",
    "DailyKlineORM",
    "AdjFactorORM",
    "BasicInfoORM",
    "TradeCalendarORM",
]
