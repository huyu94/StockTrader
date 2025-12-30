import os
from typing import List, Optional, Union
import pandas as pd
from tqdm import tqdm
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from src.fetch.providers import BaseProvider, TushareProvider
from src.storage.daily_kline_storage_mysql import DailyKlineStorageMySQL
from utils.date_helper import DateHelper

class DailyKlineFetcher:
    """
    日线行情数据获取器
    
    功能：
    1. 从数据源（如Tushare）获取日线行情数据
    2. 支持按股票代码或按日期范围获取数据
    3. 支持异步写入，提高数据获取效率
    
    更新模式：
    - 采用增量更新模式：如果数据库中已存在相同主键（ts_code + trade_date）的记录，
      则跳过该记录，不进行更新。只插入数据库中不存在的新记录。
    - 这样可以避免重复爬取已存在的数据，提高更新效率。
    """
    def __init__(
        self, 
        provider: BaseProvider,
        storage: DailyKlineStorageMySQL,
        max_write_workers: int = 2  # 写入线程池大小
        ):
        self.provider = provider
        self.storage = storage
        # 创建写入线程池
        self.write_executor = ThreadPoolExecutor(max_workers=max_write_workers, thread_name_prefix="WriteWorker")
        self.pending_writes = []  # 存储待完成的写入任务

        self._need_update_columns = ['ts_codes', 'trade_date', 'open', 'high', 'low', 'close', 'change', 'vol', 'amount']
        self._optional_columns = ['close_qfq', 'open_qfq', 'high_qfq', 'low_qfq']
        self._all_columns = self._need_update_columns + self._optional_columns
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'write_executor'):
            self.write_executor.shutdown(wait=True)
    
    def _write_task(self, df: pd.DataFrame, description: str = "") -> None:
        """
        写入任务（实例方法，避免每次创建闭包）
        
        Args:
            df: 要写入的 DataFrame
            description: 任务描述（用于日志）
        """
        try:
            df_storage = self.to_storage_format(df)
            # 异步写入时禁用进度条，避免多个进度条同时显示
            # 使用增量更新模式（跳过已存在的记录）
            self.storage.write(df_storage, show_progress=False, incremental=True)
            logger.debug(f"异步写入完成: {description}, {len(df)} 条记录")
        except Exception as e:
            logger.error(f"异步写入失败: {description}, 错误: {e}")
    
    def _write_async(self, df: pd.DataFrame, description: str = "") -> None:
        """
        异步写入数据到数据库
        
        Args:
            df: 要写入的 DataFrame
            description: 任务描述（用于日志）
        """
        if df is None or df.empty:
            return
        
        # 提交写入任务到线程池（使用实例方法，避免每次创建闭包）
        future = self.write_executor.submit(self._write_task, df, description)
        self.pending_writes.append((future, description))
    
    def _wait_all_writes(self) -> None:
        """等待所有写入任务完成"""
        if not self.pending_writes:
            return
        
        logger.info(f"等待 {len(self.pending_writes)} 个写入任务完成...")
        with tqdm(total=len(self.pending_writes), desc="写入数据", unit="批", leave=False) as pbar:
            for future, description in self.pending_writes:
                try:
                    future.result()  # 等待任务完成
                    pbar.update(1)
                except Exception as e:
                    logger.error(f"写入任务失败: {description}, 错误: {e}")
                    pbar.update(1)
        
        # 清空待完成任务列表
        self.pending_writes.clear()
        logger.info("所有写入任务已完成")
    
    def _fetch_by_code(
        self, 
        ts_codes: List[str],
        start_date: str, 
        end_date: str,
        async_write: bool = False
        ) -> pd.DataFrame:
        """
        获取指定股票的日线行情（按日期范围）
        使用 pro_bar API，一次获取全部历史数据（更快）
        
        Args:
            async_write: 是否异步写入（如果为True，每只股票的数据会异步写入）
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
            
            if async_write and not df.empty:
                # 异步写入单只股票的数据
                self._write_async(df, f"股票 {ts_code}")
            else:
                all_results.append(df)
        
        if async_write:
            return pd.DataFrame()  # 异步写入模式，不返回数据
        else:
            return pd.concat(all_results).reset_index(drop=True) if all_results else pd.DataFrame()

    def _fetch_by_date(
        self, 
        start_date: str, 
        end_date: str,
        async_write: bool = False
        ) -> pd.DataFrame:
        """
        按日期更新股票日K线数据，使用的是pro.daily接口
        
        Args:
            async_write: 是否异步写入（如果为True，每天的数据会异步写入）
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
                
                if not df.empty:
                    logger.info(f"获取到 {len(df)} 条数据")
                    
                    if async_write:
                        # 异步写入当天的数据
                        self._write_async(df, f"日期 {current_date.strftime('%Y-%m-%d')}")
                    else:
                        all_results.append(df)
                
                current_date += timedelta(days=1)
                pbar.update(1)
        
        if async_write:
            return pd.DataFrame()  # 异步写入模式，不返回数据
        else:
            return pd.concat(all_results).reset_index(drop=True) if all_results else pd.DataFrame()



    def fetch(
        self, 
        ts_codes: Optional[Union[List[str], str]] = None,
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        async_write: bool = False
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
            async_write: 是否异步写入（如果为True，数据会边爬取边写入）
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
            df = self._fetch_by_date(start_date_normalized, end_date_normalized, async_write=async_write)
        else:
            # 按代码模式：获取指定股票数据
            logger.info(f"按代码模式：获取指定股票数据")
            df = self._fetch_by_code(ts_codes_list, start_date_normalized, end_date_normalized, async_write=async_write)
        
        return df


    def to_storage_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将数据转换为存储格式
        """
        if df.empty:
            logger.warning("df 为空，没有数据可写入")
            return pd.DataFrame()
        
        available_columns = [col for col in self._all_columns if col in df.columns]

        if not available_columns:
            logger.warning("没有可用的更新列，无法写入数据库")
            return pd.DataFrame()
        
        df_to_write = df[available_columns].copy()
        
        if df_to_write.empty:
            logger.warning("没有可用的更新数据，无法写入数据库")
            return pd.DataFrame()
        
        return df_to_write


    def update(
        self, 
        ts_codes: Optional[Union[List[str], str]] = None,
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        async_write: bool = True  # 默认启用异步写入
        ) -> None:
        """
        更新股票数据
        
        支持两种调用方式：
        1. update(ts_codes="000001.SZ", start_date="2025-12-21", end_date="2025-12-25")  # 更新单只股票
        2. update(ts_codes=["000001.SZ", "000002.SZ"], start_date="2025-12-21", end_date="2025-12-25")  # 更新多只股票
        3. update(start_date="2025-12-21", end_date="2025-12-25")  # 按日期更新全市场
        
        Args:
            async_write: 是否异步写入（默认True，边爬取边写入，提高效率）
        """
        try:
            # 清空之前的待完成任务
            self.pending_writes.clear()
            
            # 开始爬取（如果 async_write=True，数据会边爬取边写入）
            df = self.fetch(ts_codes, start_date, end_date, async_write=async_write)
            
            if async_write:
                # 异步写入模式：等待所有写入任务完成
                self._wait_all_writes()
            else:
                # 同步写入模式：统一写入所有数据
                # 使用增量更新模式（跳过已存在的记录）
                if not df.empty:
                    df_storage = self.to_storage_format(df)
                    self.storage.write(df_storage, incremental=True)
        finally:
            # 确保所有任务完成（即使出错也要等待）
            if self.pending_writes:
                self._wait_all_writes()
        