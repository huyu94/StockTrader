import os
import time
import threading
from typing import Optional, Any
from numpy import kaiser
import tushare as ts
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from .base_provider import BaseProvider

load_dotenv()

# 在模块加载时就设置 NO_PROXY，确保绕过代理提高速度
# 如果系统设置了代理，tushare API 请求可能会走代理导致速度慢
os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
os.environ["no_proxy"] = os.environ["NO_PROXY"]

class TushareProvider(BaseProvider):
    """
    Tushare data provider implementation.
    使用锁确保API调用完全串行，避免IP超限问题
    """
    _instance = None
    _class_lock = threading.Lock()  # 类级别的锁，用于单例创建
    
    def __new__(cls):
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    cls._instance = super(TushareProvider, cls).__new__(cls)
                    cls._instance._init_api()
        return cls._instance

    def _init_api(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            logger.error("TUSHARE_TOKEN not found in environment variables.")
            raise ValueError("TUSHARE_TOKEN not found. Please set it in .env file.")

        # NO_PROXY 已在模块加载时设置，这里确保配置生效
        # Initialize Tushare Pro API
        self.pro = ts.pro_api(token)
        # 实例级别的锁，确保所有API调用串行
        self._api_lock = threading.Lock()
        logger.info("Tushare API initialized (NO_PROXY configured for waditu.com).")

    def query(self, api_name: str, fields: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
        """
        Execute a query against Tushare API with retry mechanism.
        使用锁确保API调用完全串行，避免IP超限问题
        
        重试策略：
        - 最多重试 3 次
        - 每次重试间隔 2 秒
        - 记录每次尝试的日志
        """
        max_retries = 3
        retry_delay = 2  # 秒
        with self._api_lock:
            for attempt in range(max_retries):
                start_time = time.time()
                try:
                    df = self.pro.query(api_name, fields=fields, **kwargs)
                    elapsed = time.time() - start_time
                    logger.debug(f"Tushare API {api_name} success. Time: {elapsed:.3f}s, Rows: {len(df) if df is not None else 0}")
                    return df
                except Exception as e:
                    elapsed = time.time() - start_time
                    
                    if attempt < max_retries - 1:
                        logger.warning(f"Tushare API {api_name} failed (attempt {attempt + 1}/{max_retries}). "
                                     f"Time: {elapsed:.3f}s. Error: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Tushare API {api_name} failed after {max_retries} attempts. "
                                   f"Time: {elapsed:.3f}s. Error: {e}")
                        raise

    def daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        使用 pro.daily API 获取股票日线数据
        """
        with self._api_lock:
            start_time = time.time()
            try:
                df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                elapsed = time.time() - start_time
                logger.debug(f"Tushare API daily success. Time: {elapsed:.3f}s, Rows: {len(df) if df is not None else 0}")
                return df
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"Tushare daily failed. Time: {elapsed:.3f}s. Error: {e}")
                raise
            finally:
                elapsed = time.time() - start_time
                logger.debug(f"Tushare API daily success. Time: {elapsed:.3f}s, Rows: {len(df) if df is not None else 0}")
                return df

    
    def pro_bar(self, 
                ts_code: str, 
                start_date: str, 
                end_date: str, 
                adj: str = "qfq", 
                freq: str = "D", 
                factors: list = ["tor","vr"], 
                adjfactor:bool = True
                ) -> pd.DataFrame:
        """
        使用 pro_bar API 获取股票K线数据（更快，一次获取全部历史）
        优势：一次调用可以获取单只股票的全部历史数据，比多次调用 pro.daily 更快
        使用锁确保API调用完全串行，避免IP超限问题
        
        :param ts_code: 股票代码
        :param start_date: 开始日期 YYYYMMDD
        :param end_date: 结束日期 YYYYMMDD
        :param adj: 复权类型，qfq=前复权，hfq=后复权，None=不复权
        :param freq: 频率，D=日线
        :param factors: 复权因子，tor=前复权因子，None=不复权因子
        :return: DataFrame，包含日线数据和复权因子（如果factors参数指定）
        """
        with self._api_lock:
            start_time = time.time()
            try:
                df = ts.pro_bar(
                    ts_code=ts_code,
                    adj=adj,
                    start_date=start_date,
                    end_date=end_date,
                    freq=freq,
                    factors=factors,
                    adjfactor=adjfactor,
                    api=self.pro
                )
                
                elapsed = time.time() - start_time
                logger.debug(f"Tushare pro_bar success for {ts_code}. Time: {elapsed:.3f}s, Rows: {len(df) if df is not None else 0}")
                
                return df if df is not None else pd.DataFrame()
                
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"Tushare pro_bar failed. Time: {elapsed:.3f}s. Error: {e}")
                raise