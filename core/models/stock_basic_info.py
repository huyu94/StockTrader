"""
股票基本信息模型

定义股票基本信息的模型结构
"""

from typing import Optional, List
from dataclasses import dataclass
import pandas as pd

from utils.date_helper import DateHelper


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
    exchange: Optional[str] = None  # 交易所
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        result = {
            'ts_code': self.ts_code,
            'symbol': self.symbol,
            'name': self.name,
        }
        
        # 可选字段
        if self.area is not None:
            result['area'] = self.area
        if self.industry is not None:
            result['industry'] = self.industry
        if self.market is not None:
            result['market'] = self.market
        if self.list_date is not None:
            result['list_date'] = self.list_date
        if self.list_status is not None:
            result['list_status'] = self.list_status
        if self.is_hs is not None:
            result['is_hs'] = self.is_hs
        if self.exchange is not None:
            result['exchange'] = self.exchange
        
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StockBasicInfo':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据，支持以下字段：
                - ts_code: 股票代码（必需）
                - symbol: 股票代码（简化，必需）
                - name: 股票名称（必需）
                - area: 地域（可选）
                - industry: 所属行业（可选）
                - market: 市场类型（可选）
                - list_date: 上市日期（可选，支持 YYYYMMDD 或 YYYY-MM-DD）
                - list_status: 上市状态（可选）
                - is_hs: 是否沪深港通标的（可选）
                - exchange: 交易所（可选）
            
        Returns:
            StockBasicInfo: 实例对象
        """
        # 标准化日期格式
        list_date = None
        if data.get('list_date'):
            list_date = DateHelper.normalize_to_yyyy_mm_dd(data['list_date'])
        
        return cls(
            ts_code=data['ts_code'],
            symbol=data.get('symbol', data['ts_code']),  # 如果没有 symbol，使用 ts_code
            name=data.get('name', ''),
            area=data.get('area'),
            industry=data.get('industry'),
            market=data.get('market'),
            list_date=list_date,
            list_status=data.get('list_status'),
            is_hs=data.get('is_hs'),
            exchange=data.get('exchange'),
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['StockBasicInfo']:
        """
        从 DataFrame 批量创建实例列表
        
        Args:
            df: DataFrame，必须包含 ts_code, symbol, name 列
            
        Returns:
            List[StockBasicInfo]: 实例对象列表
        """
        if df is None or df.empty:
            return []
        
        records = df.to_dict('records')
        return [cls.from_dict(record) for record in records]
    
    @staticmethod
    def to_dataframe(stocks: List['StockBasicInfo']) -> pd.DataFrame:
        """
        将实例列表转换为 DataFrame
        
        Args:
            stocks: StockBasicInfo 实例列表
            
        Returns:
            pd.DataFrame: DataFrame 对象
        """
        if not stocks:
            return pd.DataFrame(columns=['ts_code', 'symbol', 'name'])
        
        records = [stock.to_dict() for stock in stocks]
        return pd.DataFrame(records)