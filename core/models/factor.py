"""
复权因子数据模型

定义复权因子数据的模型结构
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class AdjFactor:
    """
    复权因子数据模型
    
    表示单只股票在单个交易日的复权因子
    """
    ts_code: str  # 股票代码
    trade_date: str  # 交易日期 (YYYY-MM-DD)
    adj_factor: float  # 复权因子
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        raise NotImplementedError("to_dict 方法待实现")
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AdjFactor':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据
            
        Returns:
            AdjFactor: 实例对象
        """
        raise NotImplementedError("from_dict 方法待实现")

