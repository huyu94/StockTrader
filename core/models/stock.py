"""
股票基本信息模型

定义股票基本信息的模型结构
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class StockBasicInfo:
    """
    股票基本信息模型
    
    表示单只股票的基本信息
    """
    ts_code: str  # 股票代码
    symbol: str  # 股票代码（简化）
    name: str  # 股票名称
    area: Optional[str] = None  # 地域
    industry: Optional[str] = None  # 所属行业
    market: Optional[str] = None  # 市场类型
    list_date: Optional[str] = None  # 上市日期 (YYYY-MM-DD)
    list_status: Optional[str] = None  # 上市状态
    is_hs: Optional[str] = None  # 是否沪深港通标的
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        raise NotImplementedError("to_dict 方法待实现")
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StockBasicInfo':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据
            
        Returns:
            StockBasicInfo: 实例对象
        """
        raise NotImplementedError("from_dict 方法待实现")

