import os
from typing import List, Optional, Union
import pandas as pd
from tqdm import tqdm
from datetime import timedelta
from loguru import logger
from src.fetch.providers import BaseProvider, TushareProvider
from src.storage.daily_kline_storage_mysql import DailyKlineStorageMySQL
from utils.date_helper import DateHelper

class DailyKlineFetcher:
    def __init__(
        self, 
        provider: BaseProvider,
        storage: DailyKlineStorageMySQL,
        ):
        self.provider = provider
        self.storage = storage
    
    def _fetch_by_code(
        self, 
        ts_codes: List[str],
        start_date: str, 
        end_date: str, 
        ) -> pd.DataFrame:
        """
        获取指定股票的日线行情（按日期范围）
        使用 pro_bar API，一次获取全部历史数据（更快）
        """
        logger.debug(f"Fetching daily kline for {ts_codes} in {start_date}-{end_date}")
        
        all_results = []
        for ts_code in ts_codes:
            df = self.provider.pro_bar(
                ts_code=ts_code, 
                start_date=start_date,
                end_date=end_date,
                adj="None", # 爬的都是不复权数据
                freq="D"
            )
            all_results.append(df)
        return pd.concat(all_results).reset_index(drop=True)

    def _fetch_by_date(
        self, 
        start_date: str, 
        end_date: str
        ) -> pd.DataFrame:
        """
        按日期更新股票日K线数据，使用的是pro.daily接口
        """
        if not start_date or not end_date:
            raise ValueError("start_date 和 end_date 必须同时提供")
        
        start_date_normalized = DateHelper.normalize_to_yyyymmdd(start_date)
        end_date_normalized = DateHelper.normalize_to_yyyymmdd(end_date)
        
        start = DateHelper.parse_to_date(start_date_normalized)
        end = DateHelper.parse_to_date(end_date_normalized)
        
        if start > end:
            raise ValueError("start_date 不能大于 end_date")
        
        total_days = (end - start).days + 1
        all_results = []
        current_date = start
        with tqdm(total=total_days, desc="查询日期", unit="天") as pbar:
            while current_date <= end:
                trade_date_str = current_date.strftime('%Y%m%d')
                pbar.set_description(f"查询日期 {current_date.strftime('%Y-%m-%d')}")
                df = self.provider.query("daily", trade_date=trade_date_str)
                logger.info(f"获取到 {len(df)} 条数据")
                if not df.empty:
                    all_results.append(df)
                current_date += timedelta(days=1)
                pbar.update(1)
        return pd.concat(all_results).reset_index(drop=True)



    def fetch(
        self, 
        ts_codes: Optional[Union[List[str], str]] = None,
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
        ) -> pd.DataFrame:
        """ 
        获取日线行情数据
        
        支持两种模式：
        1. 按股票代码：传入 ts_codes（可以是字符串或列表）
        2. 按日期范围：不传 ts_codes 或传入 None，获取全市场数据
        
        Args:
            ts_codes: 股票代码，可以是单个字符串、列表或 None
            start_date: 开始日期
            end_date: 结束日期
        """
        if not start_date or not end_date:
            raise ValueError("start_date 和 end_date 必须同时提供")

        # 统一处理 ts_codes：规范化类型转换
        if ts_codes is None:
            ts_codes_list = None
        elif isinstance(ts_codes, str):
            ts_codes_list = [ts_codes]
        elif isinstance(ts_codes, list):
            ts_codes_list = ts_codes if ts_codes else None  # 空列表视为 None
        else:
            raise TypeError(f"ts_codes 必须是 str、List[str] 或 None，当前类型: {type(ts_codes)}")

        
        start_date_normalized = DateHelper.normalize_to_yyyymmdd(start_date)
        end_date_normalized = DateHelper.normalize_to_yyyymmdd(end_date)
        
        start = DateHelper.parse_to_date(start_date_normalized)
        end = DateHelper.parse_to_date(end_date_normalized)
        
        # 判断模式
        if ts_codes_list is None or len(ts_codes_list) == 0:
            # 按日期模式：获取全市场数据
            logger.info(f"按日期模式：获取全市场数据")
            df = self._fetch_by_date(start_date_normalized, end_date_normalized)
        else:
            # 按代码模式：获取指定股票数据
            logger.info(f"按代码模式：获取指定股票数据")
            df = self._fetch_by_code(ts_codes_list, start_date_normalized, end_date_normalized)
        
        return df


    def to_storage_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将数据转换为存储格式
        """
        return df


    def update(
        self, 
        ts_codes: Optional[Union[List[str], str]] = None,
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
        ) -> None:
        """
        更新股票数据
        
        支持两种调用方式：
        1. update(ts_codes="000001.SZ", start_date="2025-12-21", end_date="2025-12-25")  # 更新单只股票
        2. update(ts_codes=["000001.SZ", "000002.SZ"], start_date="2025-12-21", end_date="2025-12-25")  # 更新多只股票
        3. update(start_date="2025-12-21", end_date="2025-12-25")  # 按日期更新全市场
        """
        # 直接传递参数，不做额外处理（让 fetch 统一处理）
        df = self.fetch(ts_codes, start_date, end_date)
        df_storage = self.to_storage_format(df)
        self.storage.write(df_storage)
        