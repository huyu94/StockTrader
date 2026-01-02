"""
历史数据补全流水线

负责补全历史股票数据
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

from tqdm import tqdm

from core.pipelines.base import BasePipeline
from core.collectors.base import BaseCollector
from core.transformers.base import BaseTransformer
from core.loaders.base import BaseLoader
from core.common.exceptions import PipelineException

# 导入各个数据源的组件
from core.collectors.basic_info import BasicInfoCollector
from core.collectors.trade_calendar import TradeCalendarCollector
from core.collectors.daily_kline import DailyKlineCollector
from core.collectors.adj_factor import AdjFactorCollector

from core.transformers.basic_info import BasicInfoTransformer
from core.transformers.trade_calendar import TradeCalendarTransformer
from core.transformers.daily_kline import DailyKlineTransformer
from core.transformers.adj_factor import AdjFactorTransformer

from core.loaders.basic_info import BasicInfoLoader
from core.loaders.trade_calendar import TradeCalendarLoader
from core.loaders.daily_kline import DailyKlineLoader
from core.loaders.adj_factor import AdjFactorLoader

from utils.date_helper import DateHelper


class HistoryPipeline(BasePipeline):
    """
    历史数据补全流水线
    
    用于补全历史数据，通常采用全量更新模式或指定日期范围更新
    
    流程：
    1. 更新 basic_info（股票基本信息，不依赖日期）
    2. 更新 trade_calendar（交易日历，依赖日期范围）
    3. 更新 daily_kline（日K线数据，依赖股票代码和日期范围）
    4. 更新 adj_factor（复权因子，依赖股票代码和日期范围）
    """
    
    def __init__(
        self,
        basic_info_collector: Optional[BaseCollector] = None,
        basic_info_transformer: Optional[BaseTransformer] = None,
        basic_info_loader: Optional[BaseLoader] = None,
        trade_calendar_collector: Optional[BaseCollector] = None,
        trade_calendar_transformer: Optional[BaseTransformer] = None,
        trade_calendar_loader: Optional[BaseLoader] = None,
        daily_kline_collector: Optional[BaseCollector] = None,
        daily_kline_transformer: Optional[BaseTransformer] = None,
        daily_kline_loader: Optional[BaseLoader] = None,
        adj_factor_collector: Optional[BaseCollector] = None,
        adj_factor_transformer: Optional[BaseTransformer] = None,
        adj_factor_loader: Optional[BaseLoader] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化历史数据补全流水线
        
        Args:
            basic_info_collector: 股票基本信息采集器（如果为 None，则使用默认的 BasicInfoCollector）
            basic_info_transformer: 股票基本信息转换器（如果为 None，则使用默认的 BasicInfoTransformer）
            basic_info_loader: 股票基本信息加载器（如果为 None，则使用默认的 BasicInfoLoader）
            trade_calendar_collector: 交易日历采集器（如果为 None，则使用默认的 TradeCalendarCollector）
            trade_calendar_transformer: 交易日历转换器（如果为 None，则使用默认的 TradeCalendarTransformer）
            trade_calendar_loader: 交易日历加载器（如果为 None，则使用默认的 TradeCalendarLoader）
            daily_kline_collector: 日K线采集器（如果为 None，则使用默认的 DailyKlineCollector）
            daily_kline_transformer: 日K线转换器（如果为 None，则使用默认的 DailyKlineTransformer）
            daily_kline_loader: 日K线加载器（如果为 None，则使用默认的 DailyKlineLoader）
            adj_factor_collector: 复权因子采集器（如果为 None，则使用默认的 AdjFactorCollector）
            adj_factor_transformer: 复权因子转换器（如果为 None，则使用默认的 AdjFactorTransformer）
            adj_factor_loader: 复权因子加载器（如果为 None，则使用默认的 AdjFactorLoader）
            config: 流水线配置字典
        """
        # 初始化 basic_info 组件
        if basic_info_collector is None:
            basic_info_collector_config = config.get("basic_info_collector", {}) if config else {}
            basic_info_collector = BasicInfoCollector(config=basic_info_collector_config)
        
        if basic_info_transformer is None:
            basic_info_transformer_config = config.get("basic_info_transformer", {}) if config else {}
            basic_info_transformer = BasicInfoTransformer(config=basic_info_transformer_config)
        
        if basic_info_loader is None:
            basic_info_loader_config = config.get("basic_info_loader", {}) if config else {}
            if 'load_strategy' not in basic_info_loader_config:
                basic_info_loader_config['load_strategy'] = 'upsert'
            basic_info_loader = BasicInfoLoader(config=basic_info_loader_config)
        
        # 初始化 trade_calendar 组件
        if trade_calendar_collector is None:
            trade_calendar_collector_config = config.get("trade_calendar_collector", {}) if config else {}
            trade_calendar_collector = TradeCalendarCollector(config=trade_calendar_collector_config)
        
        if trade_calendar_transformer is None:
            trade_calendar_transformer_config = config.get("trade_calendar_transformer", {}) if config else {}
            trade_calendar_transformer = TradeCalendarTransformer(config=trade_calendar_transformer_config)
        
        if trade_calendar_loader is None:
            trade_calendar_loader_config = config.get("trade_calendar_loader", {}) if config else {}
            if 'load_strategy' not in trade_calendar_loader_config:
                trade_calendar_loader_config['load_strategy'] = 'upsert'
            trade_calendar_loader = TradeCalendarLoader(config=trade_calendar_loader_config)
        
        # 初始化 daily_kline 组件
        if daily_kline_collector is None:
            daily_kline_collector_config = config.get("daily_kline_collector", {}) if config else {}
            daily_kline_collector = DailyKlineCollector(config=daily_kline_collector_config)
        
        if daily_kline_transformer is None:
            daily_kline_transformer_config = config.get("daily_kline_transformer", {}) if config else {}
            daily_kline_transformer = DailyKlineTransformer(config=daily_kline_transformer_config)
        
        if daily_kline_loader is None:
            daily_kline_loader_config = config.get("daily_kline_loader", {}) if config else {}
            if 'load_strategy' not in daily_kline_loader_config:
                daily_kline_loader_config['load_strategy'] = 'upsert'
            daily_kline_loader = DailyKlineLoader(config=daily_kline_loader_config)
        
        # 初始化 adj_factor 组件
        if adj_factor_collector is None:
            adj_factor_collector_config = config.get("adj_factor_collector", {}) if config else {}
            adj_factor_collector = AdjFactorCollector(config=adj_factor_collector_config)
        
        if adj_factor_transformer is None:
            adj_factor_transformer_config = config.get("adj_factor_transformer", {}) if config else {}
            adj_factor_transformer = AdjFactorTransformer(config=adj_factor_transformer_config)
        
        if adj_factor_loader is None:
            adj_factor_loader_config = config.get("adj_factor_loader", {}) if config else {}
            if 'load_strategy' not in adj_factor_loader_config:
                adj_factor_loader_config['load_strategy'] = 'upsert'
            adj_factor_loader = AdjFactorLoader(config=adj_factor_loader_config)
        
        # 保存各个组件
        self.basic_info_collector = basic_info_collector
        self.basic_info_transformer = basic_info_transformer
        self.basic_info_loader = basic_info_loader
        
        self.trade_calendar_collector = trade_calendar_collector
        self.trade_calendar_transformer = trade_calendar_transformer
        self.trade_calendar_loader = trade_calendar_loader
        
        self.daily_kline_collector = daily_kline_collector
        self.daily_kline_transformer = daily_kline_transformer
        self.daily_kline_loader = daily_kline_loader
        
        self.adj_factor_collector = adj_factor_collector
        self.adj_factor_transformer = adj_factor_transformer
        self.adj_factor_loader = adj_factor_loader

        
        # BasePipeline 需要 collector, transformer, loader，我们使用 daily_kline 作为占位符
        # 因为这是主要的数据源
        super().__init__(daily_kline_collector, daily_kline_transformer, daily_kline_loader, config)
    
    def run(self, start_date: str, end_date: str, **kwargs) -> None:
        """
        执行历史数据补全流水线
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)
            **kwargs: 其他参数
                - update_basic_info: bool, 是否更新 basic_info（默认 True）
                - update_trade_calendar: bool, 是否更新 trade_calendar（默认 True）
                - update_daily_kline: bool, 是否更新 daily_kline（默认 True）
                - update_adj_factor: bool, 是否更新 adj_factor（默认 True）
        """
        try:
            logger.info("=" * 60)
            logger.info("开始执行历史数据补全流水线")
            logger.info(f"日期范围: {start_date} ~ {end_date}")
            logger.info("=" * 60)
            
            # 标准化日期格式
            start_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(start_date)
            end_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(end_date)
            
            # 转换为 YYYYMMDD 格式（用于 API 调用）
            start_date_api = DateHelper.normalize_to_yyyymmdd(start_date)
            end_date_api = DateHelper.normalize_to_yyyymmdd(end_date)
            

            # 1. 更新 basic_info（股票基本信息）
            # 1


            # 获取更新选项
            update_basic_info = kwargs.get("update_basic_info", True)
            update_trade_calendar = kwargs.get("update_trade_calendar", True)
            update_daily_kline = kwargs.get("update_daily_kline", True)
            update_adj_factor = kwargs.get("update_adj_factor", True)
            
            if update_basic_info:
                logger.info("-" * 60)
                logger.info("步骤 1: 更新股票基本信息 (basic_info)")
                logger.info("-" * 60)
                self._update_basic_info()
            
            # 2. 更新 trade_calendar（交易日历）
            if update_trade_calendar:
                logger.info("-" * 60)
                logger.info("步骤 2: 更新交易日历 (trade_calendar)")
                logger.info("-" * 60)
                self._update_trade_calendar(start_date_api, end_date_api)
            
            # 3. 更新 daily_kline（日K线数据）
            if update_daily_kline:
                logger.info("-" * 60)
                logger.info("步骤 3: 更新日K线数据 (daily_kline)")
                logger.info("-" * 60)
                self._update_daily_kline(start_date_api, end_date_api)
            
            # 4. 更新 adj_factor（复权因子）
            if update_adj_factor:
                logger.info("-" * 60)
                logger.info("步骤 4: 更新复权因子 (adj_factor)")
                logger.info("-" * 60)
                self._update_adj_factor(start_date_api, end_date_api)
            
            logger.info("=" * 60)
            logger.info("历史数据补全流水线执行完成！")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"执行历史数据补全流水线失败: {e}")
            raise PipelineException(f"执行历史数据补全流水线失败: {e}") from e
    
    def _update_basic_info(self) -> None:
        """
        更新股票基本信息
        """
        try:
            # 1. Extract - 采集数据
            logger.info("采集股票基本信息...")
            raw_data = self.basic_info_collector.collect({})
            
            if raw_data is None or raw_data.empty:
                logger.warning("未采集到股票基本信息数据")
                return
            
            logger.info(f"✓ 采集完成，数据量: {len(raw_data)} 条")
            
            # 2. Transform - 转换数据
            logger.info("转换股票基本信息...")
            clean_data = self.basic_info_transformer.transform(raw_data)
            
            if clean_data is None or clean_data.empty:
                logger.warning("转换后的股票基本信息数据为空")
                return
            
            logger.info(f"✓ 转换完成，数据量: {len(clean_data)} 条")
            
            # 3. Load - 加载数据
            logger.info("加载股票基本信息到数据库...")
            self.basic_info_loader.load(clean_data)
            logger.info(f"✓ 加载完成，共 {len(clean_data)} 条记录")
            
        except Exception as e:
            logger.error(f"更新股票基本信息失败: {e}")
            raise
    
    def _update_trade_calendar(self, start_date: str, end_date: str) -> None:
        """
        更新交易日历
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        try:
            # 1. Extract - 采集数据
            logger.info(f"采集交易日历数据，日期范围: {start_date} ~ {end_date}...")
            raw_data = self.trade_calendar_collector.collect({
                "start_date": start_date,
                "end_date": end_date
            })
            
            if raw_data is None or raw_data.empty:
                logger.warning("未采集到交易日历数据")
                return
            
            logger.info(f"✓ 采集完成，数据量: {len(raw_data)} 条")
            
            # 2. Transform - 转换数据
            logger.info("转换交易日历数据...")
            clean_data = self.trade_calendar_transformer.transform(raw_data)
            
            if clean_data is None or clean_data.empty:
                logger.warning("转换后的交易日历数据为空")
                return
            
            logger.info(f"✓ 转换完成，数据量: {len(clean_data)} 条")
            
            # 3. Load - 加载数据
            logger.info("加载交易日历到数据库...")
            self.trade_calendar_loader.load(clean_data)
            logger.info(f"✓ 加载完成，共 {len(clean_data)} 条记录")
            
        except Exception as e:
            logger.error(f"更新交易日历失败: {e}")
            raise
    
    def _update_daily_kline(self, start_date: str, end_date: str) -> None:
        """
        更新日K线数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        try:
            # 异步线程池
            write_executor = ThreadPoolExecutor(max_workers=15, thread_name_prefix="write_thread")
            pending_writes = [] 
            ts_code_list = self.basic_info_collector.get_all_ts_codes()
            start_date = DateHelper.parse_to_date(start_date)
            end_date = DateHelper.parse_to_date(end_date)
            len_date_range = (end_date - start_date).days + 1
            with tqdm(total= len_date_range, desc="采集日K线数据") as pbar:
                for trade_date in pd.date_range(start_date, end_date):
                    trade_date_str = DateHelper.normalize_to_yyyymmdd(trade_date.strftime('%Y-%m-%d'))
                    raw_data = self.daily_kline_collector.collect({'trade_date': trade_date_str})
                    if raw_data is None or raw_data.empty:
                        continue 

                    transformed_data = self.daily_kline_transformer.transform(raw_data)
                    if transformed_data is None or transformed_data.empty:
                        continue
                    
                    future = write_executor.submit(self.daily_kline_loader.load, transformed_data)
                    pending_writes.append((future, f"日期: {trade_date_str}数据写入"))
                    pbar.update(1)

            # 等待所有写入完成
            with tqdm(total=len(pending_writes), desc="等待写入完成", unit="批", leave=False) as pbar:
                for future, desc in pending_writes:
                    try:
                        future.result()
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"写入日K线数据失败: {e}")
                        pbar.update(1)
        except Exception as e:
            logger.error(f"更新日K线数据失败: {e}")
            raise
    
    def _update_adj_factor(self, start_date: str, end_date: str) -> None:
        """
        更新复权因子
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        try:
            # 1. Extract - 采集数据
            logger.info(f"采集复权因子数据 日期范围: {start_date} ~ {end_date}...")

            ts_code_list = self.basic_info_collector.get_all_ts_codes()
            
            # 复权因子需要按股票代码逐个采集
            for ts_code in ts_code_list:
                try:
                    raw_data = self.adj_factor_collector.get_batch_stocks_adj_factor(ts_code_list)
                    logger.info(f"✓ 采集完成，数据量: {len(raw_data)} 条")
                except Exception as e:
                    logger.warning(f"采集股票 {ts_code} 的复权因子失败: {e}")
                    continue
            
            # 2. Transform - 转换数据
            logger.info("转换复权因子数据...")
            clean_data = self.adj_factor_transformer.transform(raw_data)
            
            if clean_data is None or clean_data.empty:
                logger.warning("转换后的复权因子数据为空")
                return
            
            logger.info(f"✓ 转换完成，数据量: {len(clean_data)} 条")
            
            # 3. Load - 加载数据
            logger.info("加载复权因子到数据库...")
            self.adj_factor_loader.load(clean_data)
            logger.info(f"✓ 加载完成，共 {len(clean_data)} 条记录")
            
        except Exception as e:
            logger.error(f"更新复权因子失败: {e}")
            raise

