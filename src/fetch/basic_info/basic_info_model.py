
from datetime import date
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
import pandas as pd

class StockBasicInfo(BaseModel):
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
                validated = StockBasicInfo(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records
