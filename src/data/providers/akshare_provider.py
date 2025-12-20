import akshare as ak
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from typing import Optional
from datetime import datetime

from src.data.providers.base import BaseProvider
from src.data.common.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS

class AkShareProvider(BaseProvider):
    def __init__(self):
        logger.info("AkShare API initialized")

    def _convert_ts_code_to_ak_symbol(self, ts_code: str) -> str:
        return ts_code.split('.')[0]

    def _convert_ts_code_to_sina_symbol(self, ts_code: str) -> str:
        code, exchange = ts_code.split('.')
        if exchange == 'SH':
            return f"sh{code}"
        elif exchange == 'SZ':
            return f"sz{code}"
        elif exchange == 'BJ':
            return f"bj{code}"
        return code

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_stock_basic_info(self) -> pd.DataFrame:
        try:
            df = ak.stock_zh_a_spot_em()
            def get_ts_code(row):
                code = str(row['代码'])
                if code.startswith('6'):
                    return f"{code}.SH"
                elif code.startswith('8') or code.startswith('4'):
                    return f"{code}.BJ"
                else:
                    return f"{code}.SZ"
            df['ts_code'] = df.apply(get_ts_code, axis=1)
            df['symbol'] = df['代码']
            df['name'] = df['名称']
            df['area'] = ''
            df['industry'] = ''
            df['market'] = ''
            df['list_date'] = '' 
            return df[['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date']]
        except Exception as e:
            logger.error(f"AkShare fetching basic info failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            df = ak.tool_trade_date_hist_sina()
            df['cal_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            df = df[(df['cal_date'] >= start_date) & (df['cal_date'] <= end_date)]
            df['exchange'] = 'SSE'
            df['is_open'] = 1
            return df[['exchange', 'cal_date', 'is_open']]
        except Exception as e:
            logger.error(f"AkShare fetching trade calendar failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        try:
            symbol = self._convert_ts_code_to_sina_symbol(ts_code)
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="hfq-factor")
            if df.empty:
                return None
            df = df.reset_index()
            df = df.rename(columns={'date': 'trade_date', 'hfq_factor': 'adj_factor'})
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)]
            if df.empty:
                return None
            return df[['trade_date', 'adj_factor']]
        except Exception as e:
            logger.error(f"AkShare fetching adj factor for {ts_code} failed: {e}")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_daily_k_data(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        try:
            symbol = self._convert_ts_code_to_ak_symbol(ts_code)
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
            if df.empty:
                return None
            rename_dict = {
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'vol',
                '成交额': 'amount',
                '涨跌额': 'change',
                '涨跌幅': 'pct_chg'
            }
            df = df.rename(columns=rename_dict)
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['ts_code'] = ts_code
            df['pre_close'] = df['close'].shift(1)
            needed_cols = list(DAILY_COLUMN_MAPPINGS.keys())
            for col in needed_cols:
                if col not in df.columns:
                    df[col] = 0.0
            df = df[needed_cols]
            return df
        except Exception as e:
            logger.error(f"AkShare fetching daily data for {ts_code} failed: {e}")
            raise e
