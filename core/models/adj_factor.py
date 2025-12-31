"""
复权因子数据模型

定义复权因子数据的模型结构
"""

from typing import Optional, List
from dataclasses import dataclass
import pandas as pd

from utils.date_helper import DateHelper


@dataclass
class AdjFactor:
    """
    复权因子数据模型
    
    表示单只股票在单个交易日的复权因子
    """
    ts_code: str  # 股票代码
    trade_date: str  # 交易日期 (YYYY-MM-DD)
    adj_factor: float  # 复权因子
    adj_event: Optional[str] = None  # 除权事件说明（可选）
    update_time: Optional[str] = None  # 数据入库时间（可选）
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        result = {
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'adj_factor': self.adj_factor,
        }
        
        # 可选字段
        if self.adj_event is not None:
            result['adj_event'] = self.adj_event
        if self.update_time is not None:
            result['update_time'] = self.update_time
        
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AdjFactor':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据，支持以下字段：
                - ts_code: 股票代码
                - trade_date: 交易日期（支持 YYYYMMDD 或 YYYY-MM-DD）
                - adj_factor: 复权因子
                - adj_event: 除权事件说明（可选）
                - update_time: 数据入库时间（可选，支持 YYYYMMDD 或 YYYY-MM-DD）
            
        Returns:
            AdjFactor: 实例对象
        """
        # 标准化日期格式
        trade_date = DateHelper.normalize_to_yyyy_mm_dd(data['trade_date'])
        update_time = None
        if data.get('update_time'):
            update_time = DateHelper.normalize_to_yyyy_mm_dd(data['update_time'])
        
        return cls(
            ts_code=data['ts_code'],
            trade_date=trade_date,
            adj_factor=float(data['adj_factor']) if data.get('adj_factor') is not None else 0.0,
            adj_event=data.get('adj_event'),
            update_time=update_time
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['AdjFactor']:
        """
        从 DataFrame 批量创建实例列表
        
        Args:
            df: DataFrame，必须包含 ts_code, trade_date, adj_factor 列
            
        Returns:
            List[AdjFactor]: 实例对象列表
        """
        if df is None or df.empty:
            return []
        
        records = df.to_dict('records')
        return [cls.from_dict(record) for record in records]
    
    @staticmethod
    def to_dataframe(factors: List['AdjFactor']) -> pd.DataFrame:
        """
        将实例列表转换为 DataFrame
        
        Args:
            factors: AdjFactor 实例列表
            
        Returns:
            pd.DataFrame: DataFrame 对象
        """
        if not factors:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])
        
        records = [factor.to_dict() for factor in factors]
        return pd.DataFrame(records)