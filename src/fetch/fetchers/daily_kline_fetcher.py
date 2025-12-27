import os
from typing import Optional
import pandas as pd
from loguru import logger
from src.providers import BaseProvider, TushareProvider
from utils.date_helper import DateHelper
import dotenv

dotenv.load_dotenv()

class DailyKlineFetcher:
    def __init__(self, provider_name: str = "tushare", provider: Optional[BaseProvider] = None):
        self.provider_name = provider_name
        self.provider = provider or (TushareProvider() if provider_name == "tushare" else None)
    
    def fetch_one(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取单只股票的日线行情（按日期范围）
        使用 pro_bar API，一次获取全部历史数据（更快）
        同时获取复权因子数据，无需单独爬取
        """
        logger.debug(f"Fetching daily kline for {ts_code} in {start_date}-{end_date}")
        
        # 使用 pro_bar API，一次获取全部历史数据（更快）
        # factors="tor" 表示获取前复权因子
        df = self.provider.pro_bar(
            ts_code=ts_code, 
            start_date=start_date, 
            end_date=end_date, 
            adj="qfq", 
            freq="D",
            factors=["tor", "vr"],  # 获取前复权因子
            adjfactor=True
        )
        
        if df is None or df.empty:
            logger.warning(f"No daily data found for {ts_code} in {start_date}-{end_date}")
            return pd.DataFrame()
        
            
        df = df.sort_values("trade_date").reset_index(drop=True)
        return df

    def fetch_daily_by_date(self, trade_date: str) -> pd.DataFrame:
        """
        按日期获取全市场日线行情（用于增量更新场景）
        注意：此方法使用 daily API，因为 pro_bar 不支持按日期获取全市场数据
        在增量更新时，当某个交易日缺失数据较多时，使用此方法批量获取该日所有股票数据
        
        :param trade_date: 交易日期（YYYY-MM-DD 格式，内部统一格式）
        """
        # Tushare API 需要 YYYYMMDD 格式，转换日期
        trade_date_yyyymmdd = DateHelper.normalize_to_yyyymmdd(trade_date)
        
        df = self.provider.query("daily", trade_date=trade_date_yyyymmdd)
        if df is None or df.empty:
            logger.warning(f"No daily data found for date: {trade_date}")
            return pd.DataFrame()
        return df
