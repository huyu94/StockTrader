"""
交易日历数据模型

定义交易日历数据的模型结构
"""

from typing import Optional, List
from dataclasses import dataclass
import pandas as pd

from utils.date_helper import DateHelper





@dataclass
class TradeCalendar:
    """
    交易日历数据模型
    
    表示某个日期的交易日历信息
    """
    cal_date: str  # 日历日期 (YYYY-MM-DD)
    sse_open: bool  # SSE是否交易
    szse_open: bool  # SZSE是否交易
    cffex_open: bool  # CFFEX是否交易
    shfe_open: bool  # SHFE是否交易
    czce_open: bool  # CZCE是否交易
    dce_open: bool  # DCE是否交易
    ine_open: bool  # INE是否交易
    
    def to_dict(self) -> dict:
        """
        转换为字典
        
        Returns:
            dict: 字典格式的数据
        """
        result = {
            'cal_date': self.cal_date,
            'sse_open': self.sse_open,
            'szse_open': self.szse_open,
            'cffex_open': self.cffex_open,
            'shfe_open': self.shfe_open,
            'czce_open': self.czce_open,
            'dce_open': self.dce_open,
            'ine_open': self.ine_open,
        }
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TradeCalendar':
        """
        从字典创建实例
        
        Args:
            data: 字典格式的数据，支持以下字段：
                - cal_date: 日历日期（支持 YYYYMMDD 或 YYYY-MM-DD）
                - sse_open: SSE是否交易
                - szse_open: SZSE是否交易
                - cffex_open: CFFEX是否交易
                - shfe_open: SHFE是否交易
                - czce_open: CZCE是否交易
                - dce_open: DCE是否交易
                - ine_open: INE是否交易
            
        Returns:
            TradeCalendar: 实例对象
        """
        # 标准化日期格式
        cal_date = DateHelper.normalize_to_yyyy_mm_dd(data['cal_date'])
        
        return cls(
            cal_date=cal_date,
            sse_open=data['sse_open'],
            szse_open=data['szse_open'],
            cffex_open=data['cffex_open'],
            shfe_open=data['shfe_open'],
            czce_open=data['czce_open'],
            dce_open=data['dce_open'],
            ine_open=data['ine_open'],
        )
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List['TradeCalendar']:
        """
        从 DataFrame 批量创建实例列表
        
        Args:
            df: DataFrame，必须包含 cal_date, sse_open, szse_open, cffex_open, shfe_open, czce_open, dce_open, ine_open 列
            
        Returns:
            List[TradeCalendar]: 实例对象列表
        """
        if df is None or df.empty:
            return []
        
        records = df.to_dict('records')
        return [cls.from_dict(record) for record in records]
    
    @staticmethod
    def to_dataframe(calendars: List['TradeCalendar']) -> pd.DataFrame:
        """
        将实例列表转换为 DataFrame
        
        Args:
            calendars: TradeCalendar 实例列表
            
        Returns:
            pd.DataFrame: DataFrame 对象
        """
        if not calendars:
            return pd.DataFrame(columns=['cal_date', 'sse_open', 'szse_open', 'cffex_open', 'shfe_open', 'czce_open', 'dce_open', 'ine_open'])
        
        records = [cal.to_dict() for cal in calendars]
        return pd.DataFrame(records)

