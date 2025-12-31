"""
K线数据模型

定义日K线数据的模型结构
"""

from typing import Optional, List
from dataclasses import dataclass
import pandas as pd

from utils.date_helper import DateHelper


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
        result = {
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'vol': self.vol,
            'amount': self.amount,
        }
        
        # 可选字段
        if self.change is not None:
            result['change'] = self.change
        if self.pct_chg is not None:
            result['pct_chg'] = self.pct_chg
        if self.close_qfq is not None:
            result['close_qfq'] = self.close_qfq
        if self.open_qfq is not None:
            result['open_qfq'] = self.open_qfq
        if self.high_qfq is not None:
            result['high_qfq'] = self.high_qfq
        if self.low_qfq is not None:
            result['low_qfq'] = self.low_qfq
        
        return result
    
    def to_dict_yyyymmdd(self) -> dict:
        """
        转换为字典，日期格式化为 YYYYMMDD
        
        Returns:
            dict: 字典格式的数据，trade_date 为 YYYYMMDD 格式
        """
        result = self.to_dict()
        if 'trade_date' in result:
            result['trade_date'] = DateHelper.normalize_to_yyyymmdd(result['trade_date'])
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DailyKline':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据，支持以下字段：
                - ts_code: 股票代码
                - trade_date: 交易日期（支持 YYYYMMDD 或 YYYY-MM-DD）
                - open, high, low, close: 价格数据
                - vol: 成交量
                - amount: 成交额
                - change, pct_chg: 涨跌额和涨跌幅（可选）
                - close_qfq, open_qfq, high_qfq, low_qfq: 前复权价格（可选）
            
        Returns:
            DailyKline: 实例对象
        """
        # 标准化日期格式
        trade_date = DateHelper.normalize_to_yyyy_mm_dd(data['trade_date'])
        
        return cls(
            ts_code=data['ts_code'],
            trade_date=trade_date,
            open=float(data['open']) if data.get('open') is not None else 0.0,
            high=float(data['high']) if data.get('high') is not None else 0.0,
            low=float(data['low']) if data.get('low') is not None else 0.0,
            close=float(data['close']) if data.get('close') is not None else 0.0,
            vol=float(data['vol']) if data.get('vol') is not None else 0.0,
            amount=float(data['amount']) if data.get('amount') is not None else 0.0,
            change=float(data['change']) if data.get('change') is not None else None,
            pct_chg=float(data['pct_chg']) if data.get('pct_chg') is not None else None,
            close_qfq=float(data['close_qfq']) if data.get('close_qfq') is not None else None,
            open_qfq=float(data['open_qfq']) if data.get('open_qfq') is not None else None,
            high_qfq=float(data['high_qfq']) if data.get('high_qfq') is not None else None,
            low_qfq=float(data['low_qfq']) if data.get('low_qfq') is not None else None,
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['DailyKline']:
        """
        从 DataFrame 批量创建实例列表
        
        Args:
            df: DataFrame，必须包含 ts_code, trade_date, open, high, low, close, vol, amount 列
            
        Returns:
            List[DailyKline]: 实例对象列表
        """
        if df is None or df.empty:
            return []
        
        records = df.to_dict('records')
        return [cls.from_dict(record) for record in records]
    
    @staticmethod
    def to_dataframe(klines: List['DailyKline']) -> pd.DataFrame:
        """
        将实例列表转换为 DataFrame
        
        Args:
            klines: DailyKline 实例列表
            
        Returns:
            pd.DataFrame: DataFrame 对象
        """
        if not klines:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount'])
        
        records = [kline.to_dict() for kline in klines]
        return pd.DataFrame(records)