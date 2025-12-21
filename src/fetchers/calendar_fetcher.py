import os
from typing import Optional
import pandas as pd
from loguru import logger
from src.providers import BaseProvider, TushareProvider
import dotenv
dotenv.load_dotenv()

class CalendarFetcher:
    """交易日历获取器"""
    def __init__(self, provider_name: str = "tushare", provider: Optional[BaseProvider] = None):
        self.provider_name = provider_name
        self.provider = provider or (TushareProvider() if provider_name == "tushare" else None)
        
    def fetch(self, start_date: Optional[str], end_date: Optional[str], exchange: str = "SSE") -> pd.DataFrame:
        """
        获取交易日历
        """
        if self.provider_name != "tushare":
            raise ValueError("Only tushare provider is supported for calendar fetching")
        
        # 指定返回字段，不包含 pretrade_date
        fields = "exchange,cal_date,is_open"
        
        params = {"exchange": exchange, "is_open": 1}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
            
        logger.info(f"Fetching calendar for {exchange}...")
        df = self.provider.query("trade_cal", fields=fields, **params)
        
        if df is not None and not df.empty:
            df = df.sort_values("cal_date").reset_index(drop=True)
            
        return df
