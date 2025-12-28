"""
股票数据 Pydantic 模型

定义所有股票相关数据的结构和验证规则
"""

from datetime import date, datetime, timedelta
from typing import Optional, List
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator, ConfigDict
import pandas as pd
from utils.date_helper import DateHelper
from utils.stock_code_helper import StockCodeHelper


class DailyKline(BaseModel):
    """
    日线行情数据模型（存储未复权原始股价）
    
    字段说明：
    - ts_code: 股票代码（例如：000001.SZ）
    - trade_date: 交易日期
    - open: 未复权开盘价
    - high: 未复权最高价
    - low: 未复权最低价
    - close: 未复权收盘价
    - change: 涨跌额
    - vol: 成交量（手）
    - amount: 成交额（千元）
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    ts_code: str = Field(..., description="股票代码", min_length=6, max_length=12)
    trade_date: date = Field(..., description="交易日期（MySQL DATE类型：YYYY-MM-DD）")
    open: Optional[float] = Field(..., description="未复权开盘价", ge=0)
    high: Optional[float] = Field(..., description="未复权最高价", ge=0)
    low: Optional[float] = Field(..., description="未复权最低价", ge=0)
    close: Optional[float] = Field(..., description="未复权收盘价", ge=0)
    change: Optional[float] = Field(..., description="涨跌额")
    vol: Optional[float] = Field(..., description="成交量（手）", ge=0)
    amount: Optional[float] = Field(..., description="成交额（千元）", ge=0)
    close_qfq: Optional[float] = Field(None, description="前复权收盘价", ge=0)
    open_qfq: Optional[float] = Field(None, description="前复权开盘价", ge=0)
    high_qfq: Optional[float] = Field(None, description="前复权最高价", ge=0)
    low_qfq: Optional[float] = Field(None, description="前复权最低价", ge=0)
    
    @field_validator('trade_date', mode='before')
    def parse_date(cls, v):
        return DateHelper.normalize_to_yyyy_mm_dd(v)



    def to_dict(self) -> dict:
        """转换为字典，日期格式化为MySQL DATE格式（YYYY-MM-DD）"""
        data = self.model_dump()
        data['trade_date'] = self.trade_date.strftime('%Y-%m-%d')
        return data
    
    def to_dict_yyyymmdd(self) -> dict:
        """转换为字典，日期格式化为 YYYYMMDD"""
        data = self.model_dump()
        data['trade_date'] = self.trade_date.strftime('%Y%m%d')
        return data
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
        """
        验证日线数据 DataFrame（批量验证优化版本）
        
        :param df: 原始 DataFrame
        :return: (验证通过的 DataFrame, 验证失败的记录列表)
        """
        if df is None or df.empty:
            return pd.DataFrame(), []
        
        validated_records = []
        failed_records = []
        
        # 将 DataFrame 转换为字典列表（一次性转换，比逐行转换快）
        records = df.to_dict('records')
        
        # 批量验证（使用列表推导式 + 异常处理）
        for idx, record in enumerate(records):
            try:
                validated = DailyKline(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records
