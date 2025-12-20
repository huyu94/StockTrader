import akshare as ak
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from typing import Optional
from datetime import datetime

from src.data_fetch.providers.base import BaseProvider
from src.data_fetch.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS

class AkShareProvider(BaseProvider):
    def __init__(self):
        logger.info("AkShare API initialized")

    def _convert_ts_code_to_ak_symbol(self, ts_code: str) -> str:
        """
        将 Tushare 代码 (000001.SZ) 转换为 AkShare 代码 (000001)
        用于 stock_zh_a_hist
        """
        return ts_code.split('.')[0]

    def _convert_ts_code_to_sina_symbol(self, ts_code: str) -> str:
        """
        将 Tushare 代码 (000001.SZ) 转换为 新浪代码 (sz000001)
        用于 stock_zh_a_daily
        """
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
        """
        获取股票基本信息
        使用 stock_zh_a_spot_em 接口
        """
        try:
            df = ak.stock_zh_a_spot_em()
            # 映射列名
            # AkShare返回: 序号, 代码, 名称, 最新价, ...
            # 我们需要: ts_code, symbol, name, area, industry, market, exchange, list_date
            
            # 构造 ts_code
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
            
            # AkShare 这个接口可能没有上市日期、地区、行业
            # 我们可以尝试补充，或者暂时留空
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
        """
        获取交易日历
        AkShare 的 tool_trade_date_hist_sina 返回所有交易日
        """
        try:
            df = ak.tool_trade_date_hist_sina()
            # df['trade_date'] 是 datetime.date 对象
            df['cal_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            
            # 筛选日期
            df = df[(df['cal_date'] >= start_date) & (df['cal_date'] <= end_date)]
            
            # 构造 Tushare 格式
            df['exchange'] = 'SSE' # 简化，假设都是
            df['is_open'] = 1 # 返回的都是交易日
            
            return df[['exchange', 'cal_date', 'is_open']]
        except Exception as e:
            logger.error(f"AkShare fetching trade calendar failed: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取复权因子
        使用 stock_zh_a_daily(adjust='hfq-factor')
        """
        try:
            symbol = self._convert_ts_code_to_sina_symbol(ts_code)
            # stock_zh_a_daily 返回索引为 date
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="hfq-factor")
            
            if df.empty:
                return None
                
            df = df.reset_index()
            # 重命名
            df = df.rename(columns={'date': 'trade_date', 'hfq_factor': 'adj_factor'})
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 筛选日期
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)]
            
            if df.empty:
                return None

            return df[['trade_date', 'adj_factor']]
        except Exception as e:
            logger.error(f"AkShare fetching adj factor for {ts_code} failed: {e}")
            # 如果失败，返回 None，不阻断流程（某些新股可能没有复权因子）
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))
    def get_daily_k_data(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取日线数据
        使用 stock_zh_a_hist
        """
        try:
            symbol = self._convert_ts_code_to_ak_symbol(ts_code)
            # stock_zh_a_hist 参数: symbol, period='daily', start_date='20200101', end_date='20200101'
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
            
            if df.empty:
                return None
            
            # 映射列名
            # AkShare: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
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
            
            # 转换日期
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 补充 Tushare 有但 AkShare 可能没有的列
            df['ts_code'] = ts_code
            df['pre_close'] = df['close'].shift(1) # 简单计算昨收
            
            # 筛选需要的列
            needed_cols = list(DAILY_COLUMN_MAPPINGS.keys())
            # 确保所有列都存在
            for col in needed_cols:
                if col not in df.columns:
                    df[col] = 0.0 # 缺失填充
            
            df = df[needed_cols]
            return df
        except Exception as e:
            logger.error(f"AkShare fetching daily data for {ts_code} failed: {e}")
            raise e
