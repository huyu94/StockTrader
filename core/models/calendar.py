"""
交易日历数据模型

定义交易日历数据的模型结构
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class TradeCalendar:
    """
    交易日历数据模型
    
    表示某个日期的交易日历信息
    """
    exchange: str  # 交易所代码 (SSE/SZSE)
    cal_date: str  # 日历日期 (YYYY-MM-DD)
    is_open: int  # 是否交易日 (1-是, 0-否)
    pretrade_date: Optional[str] = None  # 上一个交易日
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        raise NotImplementedError("to_dict 方法待实现")
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TradeCalendar':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据
            
        Returns:
            TradeCalendar: 实例对象
        """
        raise NotImplementedError("from_dict 方法待实现")

