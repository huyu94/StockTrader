"""
分时K线数据模型

定义分时K线数据的模型结构
"""

from typing import Optional, List
from dataclasses import dataclass
import pandas as pd

from utils.date_helper import DateHelper


@dataclass
class IntradayKline:
    """
    分时K线数据模型
    
    表示单只股票在某个时间点的分时数据
    """
    ts_code: str  # 股票代码
    trade_date: str  # 交易日期 (YYYY-MM-DD)
    time: str  # 时间 (HH:MM:SS)
    price: float  # 价格
    volume: int  # 成交量（手）
    amount: float  # 成交额（元）
    datetime: Optional[str] = None  # 完整时间戳 (YYYY-MM-DD HH:MM:SS)
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        result = {
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'time': self.time,
            'price': self.price,
            'volume': self.volume,
            'amount': self.amount,
        }
        
        # 可选字段
        if self.datetime is not None:
            result['datetime'] = self.datetime
        
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'IntradayKline':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据，支持以下字段：
                - ts_code: 股票代码
                - trade_date: 交易日期（支持 YYYYMMDD 或 YYYY-MM-DD）
                - time: 时间 (HH:MM:SS)
                - datetime: 完整时间戳（可选）
                - price: 价格
                - volume: 成交量
                - amount: 成交额
            
        Returns:
            IntradayKline: 实例对象
        """
        # 标准化日期格式
        trade_date = DateHelper.normalize_to_yyyy_mm_dd(data['trade_date'])
        
        # 构建 datetime（如果未提供）
        datetime_str = data.get('datetime')
        if datetime_str is None and 'trade_date' in data and 'time' in data:
            datetime_str = f"{trade_date} {data['time']}"
        
        return cls(
            ts_code=data['ts_code'],
            trade_date=trade_date,
            time=data.get('time', ''),
            datetime=datetime_str,
            price=float(data['price']) if data.get('price') is not None else 0.0,
            volume=int(data['volume']) if data.get('volume') is not None else 0,
            amount=float(data['amount']) if data.get('amount') is not None else 0.0,
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['IntradayKline']:
        """
        从 DataFrame 批量创建实例列表
        
        Args:
            df: DataFrame，必须包含 ts_code, trade_date, time, price, volume, amount 列
            
        Returns:
            List[IntradayKline]: 实例对象列表
        """
        if df is None or df.empty:
            return []
        
        records = df.to_dict('records')
        return [cls.from_dict(record) for record in records]
    
    @staticmethod
    def to_dataframe(klines: List['IntradayKline']) -> pd.DataFrame:
        """
        将实例列表转换为 DataFrame
        
        Args:
            klines: IntradayKline 实例列表
            
        Returns:
            pd.DataFrame: DataFrame 对象
        """
        if not klines:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'time', 'price', 'volume', 'amount'])
        
        records = [kline.to_dict() for kline in klines]
        return pd.DataFrame(records)

