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
    date: date = Field(..., description="交易日期（MySQL DATE类型：YYYY-MM-DD）")
    open: Optional[float] = Field(None, description="未复权开盘价", ge=0)
    high: Optional[float] = Field(None, description="未复权最高价", ge=0)
    low: Optional[float] = Field(None, description="未复权最低价", ge=0)
    close: Optional[float] = Field(None, description="未复权收盘价", ge=0)
    change: Optional[float] = Field(None, description="涨跌额")
    vol: Optional[float] = Field(None, description="成交量（手）", ge=0)
    amount: Optional[float] = Field(None, description="成交额（千元）", ge=0)
    close_qfq: Optional[float] = Field(None, description="前复权收盘价", ge=0)
    open_qfq: Optional[float] = Field(None, description="前复权开盘价", ge=0)
    high_qfq: Optional[float] = Field(None, description="前复权最高价", ge=0)
    low_qfq: Optional[float] = Field(None, description="前复权最低价", ge=0)
    
    def to_dict(self) -> dict:
        """转换为字典，日期格式化为MySQL DATE格式（YYYY-MM-DD）"""
        data = self.model_dump()
        data['date'] = self.date.strftime('%Y-%m-%d')
        return data
    
    def to_dict_yyyymmdd(self) -> dict:
        """转换为字典，日期格式化为 YYYYMMDD"""
        data = self.model_dump()
        data['date'] = self.date.strftime('%Y%m%d')
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


class BasicInfoData(BaseModel):
    """
    股票基本信息数据模型
    
    字段说明：
    - ts_code: 股票代码
    - symbol: 股票简称代码
    - name: 股票名称
    - area: 地域
    - industry: 行业
    - market: 市场类型（主板/创业板等）
    - list_date: 上市日期
    - list_status: 上市状态（L=上市 D=退市 P=暂停上市）
    - is_hs: 是否沪深港通标的（N=否 H=沪股通 S=深股通）
    - exchange: 交易所（SSE=上交所 SZSE=深交所）
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    ts_code: str = Field(..., description="股票代码", min_length=6, max_length=12)
    symbol: Optional[str] = Field(None, description="股票简称代码", max_length=10)
    name: Optional[str] = Field(None, description="股票名称", max_length=50)
    area: Optional[str] = Field(None, description="地域", max_length=20)
    industry: Optional[str] = Field(None, description="行业", max_length=50)
    market: Optional[str] = Field(None, description="市场类型", max_length=20)
    list_date: Optional[date] = Field(None, description="上市日期（MySQL DATE类型：YYYY-MM-DD）")
    list_status: Optional[str] = Field(None, description="上市状态", max_length=1)
    is_hs: Optional[str] = Field(None, description="是否沪深港通标的", max_length=1)
    exchange: Optional[str] = Field(None, description="交易所", max_length=10)
    

    
    def to_dict(self) -> dict:
        """转换为字典，日期格式化为MySQL DATE格式（YYYY-MM-DD）"""
        data = self.model_dump()
        if self.list_date:
            data['list_date'] = self.list_date.strftime('%Y-%m-%d')
        return data

    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
        """
        验证基本信息 DataFrame（批量验证优化版本）
        
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
                validated = BasicInfoData(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records


class TradeCalendarData(BaseModel):
    """
    交易日历数据模型
    
    字段说明：
    - exchange: 交易所代码（SSE=上交所 SZSE=深交所）
    - cal_date: 日历日期
    - is_open: 是否交易（0=休市 1=交易）
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    exchange: str = Field(..., description="交易所代码", max_length=10)
    cal_date: date = Field(..., description="日历日期（MySQL DATE类型：YYYY-MM-DD）")
    is_open: int = Field(..., description="是否交易", ge=0, le=1)
    

    
    def to_dict(self) -> dict:
        """转换为字典，日期格式化为MySQL DATE格式（YYYY-MM-DD）"""
        data = self.model_dump()
        data['cal_date'] = self.cal_date.strftime('%Y-%m-%d')
        return data
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
        """
        验证交易日历 DataFrame（批量验证优化版本）
        
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
                validated = TradeCalendarData(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records


class AdjFactorData(BaseModel):
    """
    复权因子数据模型（仅存储除权日的复权因子）
    
    字段说明：
    - ts_code: 股票代码
    - adj_date: 复权因子生效日期（除权除息日）
    - adj_factor: 复权因子（保留4位小数保证计算精度）
    - adj_event: 除权事件说明（如"10送5派1元"）
    - update_time: 数据入库时间
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    ts_code: str = Field(..., description="股票代码", min_length=6, max_length=12)
    trade_date: date = Field(..., description="复权因子生效日期（除权除息日，MySQL DATE类型：YYYY-MM-DD）")
    adj_factor: float = Field(..., description="复权因子", gt=0)
    adj_event: Optional[str] = Field(None, description="除权事件说明", max_length=50)
    update_time: Optional[date] = Field(None, description="数据入库时间（MySQL DATE类型：YYYY-MM-DD）")
    
    @field_validator('trade_date', mode='before')
    @classmethod
    def parse_adj_date(cls, v):
        """解析复权日期"""
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            if len(v) == 8 and v.isdigit():
                return datetime.strptime(v, '%Y%m%d').date()
            if len(v) == 10:
                return datetime.strptime(v, '%Y-%m-%d').date()
        raise ValueError(f"Invalid date format: {v}")
    
    @field_validator('ts_code')
    @classmethod
    def validate_ts_code(cls, v):
        """验证股票代码格式"""
        if not v:
            raise ValueError("ts_code cannot be empty")
        if '.' not in v:
            raise ValueError(f"Invalid ts_code format: {v}, should be like '000001.SZ'")
        return v.upper()
    
    def to_dict(self) -> dict:
        """转换为字典，日期格式化为MySQL DATE格式（YYYY-MM-DD）"""
        data = self.model_dump()
        data['trade_date'] = self.trade_date.strftime('%Y-%m-%d')
        if self.update_time:
            data['update_time'] = self.update_time.strftime('%Y-%m-%d')
        return data
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
        """
        验证复权因子 DataFrame（批量验证优化版本）
        
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
                validated = AdjFactorData(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records

