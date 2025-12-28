from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import date, datetime, timedelta
from typing import List, Optional
import pandas as pd

from utils.date_helper import DateHelper


class TradeCalendar(BaseModel):
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
    
    cal_date: date = Field(..., description="日历日期（MySQL DATE类型：YYYY-MM-DD）")
    sse_open: Optional[bool] = Field(None, description="SSE是否交易")
    szse_open: Optional[bool] = Field(None, description="SZSE是否交易")
    cffex_open: Optional[bool] = Field(None, description="CFFEX是否交易")
    shfe_open: Optional[bool] = Field(None, description="SHFE是否交易")
    czce_open: Optional[bool] = Field(None, description="CZCE是否交易")
    dce_open: Optional[bool] = Field(None, description="DCE是否交易")
    ine_open: Optional[bool] = Field(None, description="INE是否交易")
    

    @field_validator('cal_date', mode='before')
    def parse_cal_date(cls, v):
        return DateHelper.parse_to_str(v)

    
    def to_dict(self) -> dict:
        """转换为字典，日期格式化为MySQL DATE格式（YYYY-MM-DD）"""
        data = self.model_dump()
        data['cal_date'] = DateHelper.parse_to_str(self.cal_date)
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
                validated = TradeCalendar(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records
