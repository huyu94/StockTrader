"""
K线数据模型

定义日K线数据的模型结构
"""

from typing import Optional
from datetime import date
from dataclasses import dataclass


@dataclass
class DailyKline:
    """
    日K线数据模型
    
    表示单只股票在单个交易日的K线数据
    """
    ts_code: str  # 股票代码
    trade_date: str  # 交易日期 (YYYY-MM-DD)
    open: float  # 开盘价
    high: float  # 最高价
    low: float  # 最低价
    close: float  # 收盘价
    vol: float  # 成交量
    amount: float  # 成交额
    change: Optional[float] = None  # 涨跌额
    pct_chg: Optional[float] = None  # 涨跌幅
    close_qfq: Optional[float] = None  # 前复权收盘价
    open_qfq: Optional[float] = None  # 前复权开盘价
    high_qfq: Optional[float] = None  # 前复权最高价
    low_qfq: Optional[float] = None  # 前复权最低价
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        raise NotImplementedError("to_dict 方法待实现")
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DailyKline':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据
            
        Returns:
            DailyKline: 实例对象
        """
        raise NotImplementedError("from_dict 方法待实现")

