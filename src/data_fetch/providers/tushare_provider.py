import tushare as ts
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from typing import Optional

from src.config import TUSHARE_TOKEN
from src.data_fetch.providers.base import BaseProvider
from src.data_fetch.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS

class TushareProvider(BaseProvider):
    def __init__(self):
        self.pro = ts.pro_api(TUSHARE_TOKEN)
        logger.info("Tushare API initialized")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_stock_basic_info(self) -> pd.DataFrame:
        """
        获取股票基本信息
        """
        try:
            # 兼容原逻辑，exchange如果不传则默认获取所有，但tushare pro.stock_basic如果不传exchange，默认是SSE/SZSE/BSE吗？
            # 原逻辑是分三次获取拼起来的。
            # 为了简化，我们先尝试一次性获取，如果不行再分批。
            # Tushare pro.stock_basic 不传exchange会返回所有上市股票
            df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,exchange,list_date')
            return df
        except Exception as e:
            logger.error(f"Tushare fetching basic info failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            df = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
            return df
        except Exception as e:
            logger.error(f"Tushare fetching trade calendar failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        try:
            df = self.pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df.empty:
                return None
            
            # 格式化日期
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.sort_values(by='trade_date')
            
            # 筛选列
            if set(ADJ_FACTOR_COLUMN_MAPPINGS.keys()).issubset(df.columns):
                 df = df[list(ADJ_FACTOR_COLUMN_MAPPINGS.keys())]
            
            return df
        except Exception as e:
            logger.error(f"Tushare fetching adj factor for {ts_code} failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_daily_k_data(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        try:
            df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df.empty:
                return None
            
            # 格式化日期
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.sort_values(by='trade_date')
            
            # 筛选列
            if set(DAILY_COLUMN_MAPPINGS.keys()).issubset(df.columns):
                df = df[list(DAILY_COLUMN_MAPPINGS.keys())]
            
            return df
        except Exception as e:
            logger.error(f"Tushare fetching daily data for {ts_code} failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_dc_index(self, trade_date: str, fields: Optional[str] = None) -> pd.DataFrame:
        try:
            if fields:
                df = self.pro.dc_index(trade_date=trade_date, fields=fields)
            else:
                df = self.pro.dc_index(trade_date=trade_date)
            return df
        except Exception as e:
            logger.error(f"Tushare fetching dc_index {trade_date} failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_dc_member(self, trade_date: str, ts_code: str) -> pd.DataFrame:
        try:
            df = self.pro.dc_member(trade_date=trade_date, ts_code=ts_code)
            return df
        except Exception as e:
            logger.error(f"Tushare fetching dc_member {ts_code} {trade_date} failed: {e}")
            raise e
