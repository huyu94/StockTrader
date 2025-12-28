from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
import pandas as pd


class AdjFactor(BaseModel):
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
                validated = AdjFactor(**record)
                validated_records.append(validated.to_dict())
            except Exception as e:
                failed_records.append({
                    'index': df.index[idx] if idx < len(df.index) else idx,
                    'data': record,
                    'error': str(e)
                })
        
        validated_df = pd.DataFrame(validated_records) if validated_records else pd.DataFrame()
        return validated_df, failed_records

