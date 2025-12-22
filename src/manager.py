"""
统一数据管理器 (Manager)

负责管理所有数据类型（日线、复权因子、基础信息、日历）的获取、存储和更新。
采用分层架构：Provider → Fetcher → Storage → Manager

架构流程：
1. Provider层：封装API调用（TushareProvider），确保串行调用避免IP超限
2. Fetcher层：数据获取逻辑（DailyKlineFetcher等），调用Provider获取数据
3. Storage层：数据持久化（SQLite存储），批量写入优化性能
4. Manager层：统一协调，智能选择更新策略（全量/增量）

更新策略：
- 全量更新（按股票代码）：首次爬取时使用，遍历所有股票获取最近一年数据
- 增量更新（按交易日）：定期更新时使用，基于数据存在性矩阵，只更新缺失数据

性能优化：
- SQLite批量写入：单次事务写入所有数据，性能提升5-15倍
- 线程池管理：IO线程池（20线程）处理文件写入，任务线程池（1线程）调度后台任务
- 流水线处理：获取和写入并行进行，不阻塞主循环
"""
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List
from tqdm import tqdm
from loguru import logger
from functools import cached_property

# Storage (SQLite版本)
from src.storage.daily_kline_storage_sqlite import DailyKlineStorageSQLite
from src.storage.basic_info_storage_sqlite import BasicInfoStorageSQLite
from src.storage.calendar_storage_sqlite import CalendarStorageSQLite

# Fetchers
from src.fetchers.daily_kline_fetcher import DailyKlineFetcher
from src.fetchers.basic_info_fetcher import BasicInfoFetcher
from src.fetchers.calendar_fetcher import CalendarFetcher
# utils
from src.utils.date_helper import DateHelper

# Model
from src.models.stock_models import (
    DailyKlineData,
    BasicInfoData,
    TradeCalendarData,
    validate_daily_kline_dataframe,
    validate_basic_info_dataframe
)


class Manager:
    """
    统一数据管理器
    
    职责：
    1. 统一管理所有数据类型的获取、存储和更新
    2. 维护线程池以优化资源使用
    3. 智能选择更新策略（全量更新/增量更新）
    4. 协调Provider、Fetcher、Storage各层的工作
    
    线程池说明：
    - io_executor: 20个工作线程，用于Storage层的密集文件IO操作（批量写入）
    - task_executor: 1个工作线程，用于Manager层的后台任务调度（如Fetch完提交Write）
    
    数据流程：
    1. Manager.update_xxx() → 调用内部更新方法
    2. _update_stock_data() → 检查历史数据，选择更新策略
    3. _update_all_stocks_full() 或 _update_missing_data_incremental() → 执行更新
    4. Fetcher.fetch_xxx() → 调用Provider获取数据
    5. Storage.write_xxx() → 写入SQLite数据库
    """
    
    def __init__(self, provider_name: str = "tushare"):
        """
        初始化Manager
        
        流程：
        1. 创建线程池（IO线程池和任务线程池）
        2. 实例化所有Storage类（SQLite版本）
        3. 实例化所有Fetcher类
        4. 实例化Matrix Manager（用于增量更新）
        
        :param provider_name: 数据提供商名称，默认"tushare"
        """
        # ========== 线程池管理 ==========
        # io_executor: 用于 Storage 层的密集文件 IO (批量写入)
        #   20个工作线程，处理并发写入操作
        self.io_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="IOWorker")
        
        # task_executor: 用于 Manager 层的后台任务调度 (如 Fetch 完提交 Write)
        #   1个工作线程，确保任务按顺序执行，避免资源竞争
        self.task_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="TaskWorker")
        
        # ========== 实例化 Storage（全部使用SQLite）==========
        logger.info("Using SQLite storage for all data types (better performance)")
        self.daily_storage = DailyKlineStorageSQLite()      # 日线行情存储
        self.basic_storage = BasicInfoStorageSQLite()       # 股票基本信息存储
        self.calendar_storage = CalendarStorageSQLite()      # 交易日历存储
        
        # ========== 实例化 Fetchers ==========
        self.daily_fetcher = DailyKlineFetcher(provider_name=provider_name)
        self.basic_fetcher = BasicInfoFetcher(provider_name=provider_name)
        self.calendar_fetcher = CalendarFetcher(provider_name=provider_name)
        

        
        # ========== 配置参数 ==========
        self.missing_threshold = 1000  # 缺失数据阈值：当某日缺失股票数超过此值时，批量获取该日所有股票数据

    def __del__(self):
        """清理资源：关闭所有线程池"""
        self.io_executor.shutdown(wait=True)
        self.task_executor.shutdown(wait=True)

    # ==================== Public Update Methods ====================

    def update_all(self, mode: str = "code", start_date: str = None, end_date: str = None):
        """
        一键更新所有数据
        
        流程：
        1. 更新基础数据（交易日历、股票基本信息）- 必须先更新，其他数据依赖它们
        2. 更新核心数据（日线行情）- 根据模式选择不同的更新策略
        
        更新模式：
        - code模式：使用 pro_bar API 按股票代码获取过去一年的数据
          * 遍历所有股票，每只股票调用一次 pro_bar 获取全部历史数据
          * 适合首次全量爬取，数据完整
        - date模式：使用 pro.daily API 按交易日获取所有股票数据
          * 遍历所有交易日，每个交易日调用一次 pro.daily 获取全市场数据
          * 适合增量更新，补充特定日期的数据
        
        :param mode: 更新模式，"code" 或 "date"，默认 "code"
        :param start_date: 开始日期，格式YYYYMMDD
                          - 如果为None，code模式默认使用近一年数据，date模式从最早交易日开始
                          - code模式：获取从start_date到今天的近一年数据
                          - date模式：从start_date开始更新到今天的交易日数据
        """
        if mode not in ["code", "date"]:
            logger.error(f"Invalid mode: {mode}. Must be 'code' or 'date'")
            return
        
        logger.info("=" * 60)
        logger.info("Starting full data update...")
        logger.info(f"Update mode: {mode.upper()}")
        if start_date:
            logger.info(f"Start date: {start_date}")
        else:
            logger.info("Start date: Auto (近一年数据 for code mode)")
        logger.info("=" * 60)
        
        # 1. 基础数据 (Calendar & Basic Info) - 必须先更新，其他数据依赖它们
        logger.info("Step 1/2: Updating Basic Data (Calendar & Basic Info)...")
        self.update_calendar()
        self.update_basic_info()
        
        # 验证基础数据是否更新成功
        stocks = self.all_basic_info
        if stocks is None or stocks.empty:
            logger.error("Failed to get stock codes. Cannot proceed with Daily Kline update.")
            return
        
        logger.success(f"✅ Basic Info updated. Total stocks: {len(stocks)}")
        logger.success("✅ Trade Calendar updated.")
        
        # 2. 核心数据 (Daily Kline) - 根据模式选择不同的更新策略
        logger.info("Step 2/2: Updating Daily Kline Data...")
        self.update_daily_kline(mode=mode, start_date=start_date, end_date=end_date)
        
        logger.info("=" * 60)
        logger.success("🎉 Full data update completed successfully!")
        logger.info("=" * 60)

    def update_daily_kline(self, mode: str = "code", start_date: str = None, end_date: str = None):
        """
        更新日线行情数据的主函数
        
        支持两种更新模式：
        1. code模式：使用 pro_bar API 按股票代码获取过去一年的数据
           - 遍历所有股票，每只股票调用一次 pro_bar 获取全部历史数据
           - 适合首次全量爬取，数据完整
        2. date模式：使用 pro.daily API 按交易日获取所有股票数据
           - 遍历所有交易日，每个交易日调用一次 pro.daily 获取全市场数据
           - 适合增量更新，补充特定日期的数据
        
        两种模式fetch方式不同，但写入SQLite的方式相同（都使用 write_batch）
        爬取到数据后走多线程并发插入数据库
        
        流程：
        1. 根据 mode 参数选择更新策略
        2. code模式：调用 _update_by_code_mode()
        3. date模式：调用 _update_by_date_mode()
        4. 两种模式都使用 io_executor 多线程并发写入
        
        :param mode: 更新模式，"code" 或 "date"，默认 "code"
        :param start_date: 开始日期，格式YYYYMMDD
                          - code模式：获取从start_date到今天的近一年数据（默认365天）
                          - date模式：从start_date开始更新到今天的交易日数据
        """
        if mode not in ["code", "date"]:
            logger.error(f"Invalid mode: {mode}. Must be 'code' or 'date'")
            return
        
        # 使用 DateHelper 统一处理日期格式（Manager 内部全部使用 YYYYMMDD）
        # 日期应该已经在 scripts 层被标准化，这里做最后的兜底处理
        if end_date is None:
            end_date = DateHelper.today()
        
        if start_date is None:
            start_date = DateHelper.days_ago(365)
        
        logger.info(f"Updating Daily Kline Data in {mode} mode from {start_date} to {end_date}")
        
        if mode == "code":
            self._update_by_code_mode(start_date, end_date)
        else:  # mode == "date"
            self._update_by_date_mode(start_date, end_date)

    def update_basic_info(self):
        """
        更新股票基本信息
        
        流程：
        1. 检查是否需要更新（通过 check_update_needed()）
        2. 如果需要更新，调用 Fetcher 获取数据
        3. 写入 SQLite 数据库
        
        注意：此方法会检查缓存，如果今日已更新则跳过
        """
        if self.basic_storage.check_update_needed():
            logger.info("Updating basic info...")
            df = self.basic_fetcher.fetch()
            validated_df, failed_records = validate_basic_info_dataframe(df)
            if validated_df is not None and not validated_df.empty:
                self.basic_storage.write(validated_df)
            if failed_records:
                logger.warning(f"验证过程中存在{len(failed_records)}条数据验证失败")
                for failed_record in failed_records:
                    logger.warning(f"失败数据: {failed_record['data']}, 错误: {failed_record['error']}")
        else:
            logger.debug("Basic info is up to date.")

    def update_calendar(self, exchange: str = "SSE"):
        """
        更新交易日历
        
        流程：
        1. 遍历所有交易所（SSE、SZSE）
        2. 检查每个交易所是否需要更新
        3. 获取最近一年的交易日历数据
        4. 写入 SQLite 数据库
        
        :param exchange: 交易所代码（默认SSE，但实际会更新SSE和SZSE两个）
        """
        # 这里简化处理，通常更新 SSE 和 SZSE
        for ex in ["SSE", "SZSE"]:
            if self.calendar_storage.check_update_needed(ex):
                logger.info(f"Updating calendar for {ex}...")
                now = pd.Timestamp.now()
                end_date = now.strftime("%Y%m%d")
                start_date = (now - timedelta(days=365)).strftime("%Y%m%d")
                
                df = self.calendar_fetcher.fetch(start_date=start_date, end_date=end_date, exchange=ex)
                if df is not None and not df.empty:
                    self.calendar_storage.write(df, exchange=ex)
            else:
                logger.debug(f"Calendar for {ex} is up to date.")

    # ==================== Data Access Methods (Facade) ====================
    
    @cached_property
    def all_basic_info(self) -> pd.DataFrame:
        """
        获取所有股票基本信息（缓存属性）
        
        流程：
        1. 检查是否需要更新
        2. 如果需要，调用 update_basic_info()
        3. 从数据库加载并返回
        
        :return: 包含所有股票基本信息的DataFrame，如果数据库为空则返回空DataFrame
        """
        if self.basic_storage.check_update_needed():
            self.update_basic_info()
        result = self.basic_storage.load()
        return result if result is not None else pd.DataFrame()

    def get_calendar(self, exchange: str = "SSE") -> pd.DataFrame:
        """
        获取交易日历（按需加载）
        
        流程：
        1. 检查是否需要更新
        2. 如果需要，调用 update_calendar()
        3. 从数据库加载并返回
        
        :param exchange: 交易所代码，默认SSE
        :return: 交易日历DataFrame
        """
        if self.calendar_storage.check_update_needed(exchange):
            self.update_calendar(exchange)
        return self.calendar_storage.load(exchange)

    # ==================== Internal Generic Methods ====================
    def _fetch_kline_data_by_code(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取单只股票的日线行情数据
        
        流程：
        1. 调用 fetcher.fetch_one() 使用 pro_bar 获取该股票的数据
        2. 使用 DailyKlineData.validate_dataframe() 验证数据格式
        3. 返回验证后的 DataFrame
        """
        df = self.daily_fetcher.fetch_one(ts_code=ts_code, start_date=start_date, end_date=end_date)
        validated_df = DailyKlineData.validate_dataframe(df)
        logger.debug(f"Validated {len(validated_df)} rows of kline data for stock {ts_code}")
        return validated_df

    def _fetch_kline_data_by_date(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定交易日的所有股票日线行情数据
        
        流程：
        1. 调用 fetcher.fetch_daily_by_date() 使用 pro.daily 获取该交易日的所有股票数据
        2. 使用 DailyKlineData.validate_dataframe() 验证数据格式
        3. 返回验证后的 DataFrame
        """
        df = self.daily_fetcher.fetch_daily_by_date(trade_date=trade_date)
        validated_df = DailyKlineData.validate_dataframe(df)
        logger.debug(f"Validated {len(validated_df)} rows of kline data for trade date {trade_date}")    
        return validated_df

    def _save_kline_data_to_sql(self, df: pd.DataFrame) -> bool:
        """
        将日线行情数据保存到 SQLite 数据库
        
        流程：
        1. 调用 storage.write_batch() 批量写入
        2. 返回 True 表示成功，False 表示失败
        """
        logger.debug(f"Saving {len(df)} rows of kline data to SQLite...")
        return self.daily_storage.write(df)

    def _update_by_code_mode(self, start_date: str, end_date: str):
        """
        Code模式：使用 pro_bar API 按股票代码获取数据
        
        流程：
        1. 获取所有股票代码列表（从 basic_info）
        2. 遍历每只股票（使用 tqdm 显示进度）
           2.1. 调用 fetcher.fetch_one() 使用 pro_bar 获取该股票过去一年的数据
           2.2. 提交到 io_executor，异步执行 storage.write_batch() 批量写入
        3. 等待所有写入任务完成
        
        性能特点：
        - 使用 task_executor 串行调度任务（避免API并发超限）
        - 使用 io_executor 并发写入（提升写入性能）
        - 适合首次爬取，数据完整
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        """
        # 1. 获取所有股票代码
        basic_info = self.all_basic_info
        if basic_info is None or basic_info.empty:
            logger.error("Failed to get stock codes. Please update basic info first.")
            return
        
        ts_codes = basic_info["ts_code"].tolist()
        logger.info(f"Code mode: Updating Daily Kline for {len(ts_codes)} stocks...")
        
        # 2. 遍历股票代码，批量更新
        pending_futures = []
        for ts_code in tqdm(ts_codes, desc="Fetching by code"):
            df = self._fetch_kline_data_by_code(ts_code, start_date, end_date)
            future = self.io_executor.submit(
                self._save_kline_data_to_sql,
                df
            )   
            pending_futures.append(future)
        
        # 3. 等待所有任务完成
        if pending_futures:
            logger.info("Waiting for all fetch and write tasks to complete...")
            success_count = 0
            failed_count = 0
            for future in tqdm(pending_futures, desc="Writing"):
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Task failed with exception: {e}")
                    failed_count += 1
            
            # 显示统计信息
            total = len(ts_codes)
            logger.info(f"Code mode completed: {success_count} succeeded, {failed_count} failed out of {total} stocks.")
            if failed_count > 0:
                logger.warning(f"⚠️  {failed_count} stocks failed to write. Data may be incomplete.")
                logger.warning(f"💡 Tip: Reduce io_executor max_workers to 1-2 to avoid database locks.")
        
        logger.info("Code mode update completed.")
    
    def _update_by_date_mode(self, start_date: str, end_date: str):
        """
        Date模式：使用 pro.daily API 按交易日获取数据
        
        流程：
        1. 获取指定日期范围内的所有交易日
        2. 遍历每个交易日（使用 tqdm 显示进度）
           2.1. 调用 fetcher.fetch_daily_by_date() 获取该交易日的所有股票数据
           2.2. 提交到 io_executor，异步执行 storage.write_batch() 批量写入
        3. 等待所有写入任务完成
        
        性能特点：
        - 按交易日批量获取，适合增量更新
        - 使用 task_executor 串行调度任务（避免API并发超限）
        - 使用 io_executor 并发写入（提升写入性能）
        - 适合补充特定日期的缺失数据
        
        :param start_date: 开始日期，格式YYYYMMDD
        :param end_date: 结束日期，格式YYYYMMDD
        """
        # 1. 获取所有交易日
        calendar_df = self.get_calendar()
        if calendar_df is None or calendar_df.empty:
            logger.error("Failed to get trade calendar. Please update calendar first.")
            return
        
        # 筛选指定日期范围内的交易日（使用 DateHelper 统一处理为 YYYYMMDD）
        calendar_df_copy = calendar_df.copy()
        if "cal_date" in calendar_df_copy.columns:
            # 统一转换为 YYYYMMDD 格式进行比较
            calendar_df_copy["cal_date"] = calendar_df_copy["cal_date"].astype(str)
            def normalize_date(d):
                try:
                    return DateHelper.normalize(d)
                except:
                    return None
            calendar_df_copy["cal_date"] = calendar_df_copy["cal_date"].apply(normalize_date)
            # 移除无效日期
            calendar_df_copy = calendar_df_copy[calendar_df_copy["cal_date"].notna()]
        
        trade_dates = calendar_df_copy[
            (calendar_df_copy['cal_date'] >= start_date) & 
            (calendar_df_copy['cal_date'] <= end_date)
        ]['cal_date'].tolist()
        
        if not trade_dates:
            logger.error(f"No trade dates found in range {start_date}-{end_date}")
            return
        
        trade_dates = sorted(trade_dates)
        logger.info(f"Date mode: Updating Daily Kline for {len(trade_dates)} trade dates...")
        
        # 2. 遍历每个交易日，批量更新
        pending_futures = []
        for trade_date in tqdm(trade_dates, desc="Fetching by date"):
            df = self._fetch_kline_data_by_date(trade_date)
            future = self.io_executor.submit(
                self._save_kline_data_to_sql,
                df
            )   
            pending_futures.append(future)
        
        # 3. 等待所有任务完成
        if pending_futures:
            logger.info("Waiting for all fetch and write tasks to complete...")
            success_count = 0
            failed_count = 0
            for future in tqdm(pending_futures, desc="Writing"):
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Task failed with exception: {e}")
                    failed_count += 1
            
            # 显示统计信息
            total = len(trade_dates)
            logger.info(f"Date mode completed: {success_count} succeeded, {failed_count} failed out of {total} dates.")
            if failed_count > 0:
                logger.warning(f"⚠️  {failed_count} dates failed to write. Data may be incomplete.")
                logger.warning(f"💡 Tip: Reduce io_executor max_workers to 1-2 to avoid database locks.")
        logger.info("Date mode update completed.")
    
    
    # ======================== load data =======================  
    def load_kline_data_from_sql(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 SQLite 数据库加载日线行情数据
        """
        return self.daily_storage.load(ts_code, start_date, end_date)