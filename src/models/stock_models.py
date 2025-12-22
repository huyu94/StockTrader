"""
股票数据 Pydantic 模型

定义所有股票相关数据的结构和验证规则
"""

from datetime import date, datetime
from typing import Optional, List
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator, ConfigDict
import pandas as pd


class DailyKlineData(BaseModel):
    """
    日线行情数据模型
    
    字段说明：
    - ts_code: 股票代码（例如：000001.SZ）
    - trade_date: 交易日期
    - open: 开盘价
    - high: 最高价
    - low: 最低价
    - close: 收盘价
    - pre_close: 前收盘价
    - change: 涨跌额
    - pct_chg: 涨跌幅（%）
    - vol: 成交量（手）
    - amount: 成交额（千元）
    - adj_factor: 复权因子
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    ts_code: str = Field(..., description="股票代码", min_length=6, max_length=12)
    trade_date: date = Field(..., description="交易日期")
    open: Optional[float] = Field(None, description="开盘价", ge=0)
    high: Optional[float] = Field(None, description="最高价", ge=0)
    low: Optional[float] = Field(None, description="最低价", ge=0)
    close: Optional[float] = Field(None, description="收盘价", ge=0)
    pre_close: Optional[float] = Field(None, description="前收盘价", ge=0)
    change: Optional[float] = Field(None, description="涨跌额")
    pct_chg: Optional[float] = Field(None, description="涨跌幅（%）")
    vol: Optional[float] = Field(None, description="成交量（手）", ge=0)
    amount: Optional[float] = Field(None, description="成交额（千元）", ge=0)
    adj_factor: Optional[float] = Field(None, description="复权因子", gt=0)
    
    @field_validator('trade_date', mode='before')
    @classmethod
    def parse_trade_date(cls, v):
        """解析交易日期，支持多种格式"""
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            # 支持 YYYYMMDD 格式
            if len(v) == 8 and v.isdigit():
                return datetime.strptime(v, '%Y%m%d').date()
            # 支持 YYYY-MM-DD 格式
            if len(v) == 10:
                return datetime.strptime(v, '%Y-%m-%d').date()
        raise ValueError(f"Invalid date format: {v}")
    
    @field_validator('ts_code')
    @classmethod
    def validate_ts_code(cls, v):
        """验证股票代码格式"""
        if not v:
            raise ValueError("ts_code cannot be empty")
        # 基本格式检查：应该包含 . 分隔符
        if '.' not in v:
            raise ValueError(f"Invalid ts_code format: {v}, should be like '000001.SZ'")
        return v.upper()
    
    @field_validator('high', 'low', 'open', 'close')
    @classmethod
    def validate_price_logic(cls, v, info):
        """验证价格逻辑关系"""
        # 注意：这里只能验证单个字段，跨字段验证需要在 model_validator 中进行
        if v is not None and v < 0:
            raise ValueError(f"{info.field_name} must be non-negative")
        return v
    
    def to_dict(self) -> dict:
        """转换为字典，日期格式化为字符串"""
        data = self.model_dump()
        data['trade_date'] = self.trade_date.strftime('%Y-%m-%d')
        return data
    
    def to_dict_yyyymmdd(self) -> dict:
        """转换为字典，日期格式化为 YYYYMMDD"""
        data = self.model_dump()
        data['trade_date'] = self.trade_date.strftime('%Y%m%d')
        return data


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
    list_date: Optional[date] = Field(None, description="上市日期")
    list_status: Optional[str] = Field(None, description="上市状态", max_length=1)
    is_hs: Optional[str] = Field(None, description="是否沪深港通标的", max_length=1)
    exchange: Optional[str] = Field(None, description="交易所", max_length=10)
    
    @field_validator('list_date', mode='before')
    @classmethod
    def parse_list_date(cls, v):
        """解析上市日期"""
        if v is None or v == '':
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            if len(v) == 8 and v.isdigit():
                return datetime.strptime(v, '%Y%m%d').date()
            if len(v) == 10:
                return datetime.strptime(v, '%Y-%m-%d').date()
        return None
    
    @field_validator('ts_code')
    @classmethod
    def validate_ts_code(cls, v):
        """验证股票代码格式"""
        if not v:
            raise ValueError("ts_code cannot be empty")
        if '.' not in v:
            raise ValueError(f"Invalid ts_code format: {v}")
        return v.upper()
    
    def to_dict(self) -> dict:
        """转换为字典"""
        data = self.model_dump()
        if self.list_date:
            data['list_date'] = self.list_date.strftime('%Y-%m-%d')
        return data


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
    cal_date: date = Field(..., description="日历日期")
    is_open: int = Field(..., description="是否交易", ge=0, le=1)
    
    @field_validator('cal_date', mode='before')
    @classmethod
    def parse_cal_date(cls, v):
        """解析日期"""
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
    
    def to_dict(self) -> dict:
        """转换为字典"""
        data = self.model_dump()
        data['cal_date'] = self.cal_date.strftime('%Y-%m-%d')
        return data



# 辅助函数

def validate_daily_kline_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
    """
    验证日线数据 DataFrame
    
    :param df: 原始 DataFrame
    :return: (验证通过的 DataFrame, 验证失败的记录列表)
    """
    if df is None or df.empty:
        return pd.DataFrame(), []
    
    validated_records = []
    failed_records = []
    
    for idx, row in df.iterrows():
        try:
            record = row.to_dict()
            validated = DailyKlineData(**record)
            validated_records.append(validated.to_dict())
        except Exception as e:
            failed_records.append({
                'index': idx,
                'data': row.to_dict(),
                'error': str(e)
            })
    
    validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
    return validated_df, failed_records


def validate_basic_info_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
    """
    验证基本信息 DataFrame
    
    :param df: 原始 DataFrame
    :return: (验证通过的 DataFrame, 验证失败的记录列表)
    """
    if df is None or df.empty:
        return pd.DataFrame(), []
    
    validated_records = []
    failed_records = []
    
    for idx, row in df.iterrows():
        try:
            record = row.to_dict()
            validated = BasicInfoData(**record)
            validated_records.append(validated.to_dict())
        except Exception as e:
            failed_records.append({
                'index': idx,
                'data': row.to_dict(),
                'error': str(e)
            })
    
    validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
    return validated_df, failed_records

