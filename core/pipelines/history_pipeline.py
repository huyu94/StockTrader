"""
历史数据补全流水线

负责补全历史股票数据
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
import math
import signal
import sys

from tqdm import tqdm

from core.calculators.qfq_calculator import QFQCalculator
from core.loaders import adj_factor
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
    
    def __init__(self):
        """
        初始化历史数据补全流水线
        """
        self.write_executor = ThreadPoolExecutor(max_workers=15, thread_name_prefix="write_thread")
        self.pending_writes = []
        self._shutdown_requested = False
        
        # 注册信号处理器，用于优雅关闭
        # Windows 只支持 SIGINT，不支持 SIGTERM
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, self._signal_handler)
        
        # 因为这是主要的数据源
        super().__init__()
    
    def _signal_handler(self, signum, frame):
        """信号处理器，用于捕获 Ctrl+C 等中断信号"""
        logger.warning(f"收到中断信号 ({signum})，正在优雅关闭...")
        self._shutdown_requested = True
        self._graceful_shutdown()
    
    def _graceful_shutdown(self):
        """优雅关闭线程池"""
        if self.write_executor is None:
            return
        
        logger.info("正在关闭写入线程池...")
        # 取消所有未开始的任务
        for future, desc in self.pending_writes:
            if not future.done():
                future.cancel()
                logger.debug(f"已取消任务: {desc}")
        
        # 关闭线程池，等待正在执行的任务完成（最多等待30秒）
        self.write_executor.shutdown(wait=False)
        logger.info("写入线程池已关闭")
    
    def __del__(self):
        if hasattr(self, 'write_executor') and self.write_executor is not None:
            self._graceful_shutdown()



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


            # 获取更新选项
            update_basic_info = kwargs.get("update_basic_info", True)
            update_trade_calendar = kwargs.get("update_trade_calendar", True)
            update_daily_kline = kwargs.get("update_daily_kline", True)
            update_adj_factor = kwargs.get("update_adj_factor", True)
            update_qfq_data = kwargs.get("update_qfq_data", True)
            
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
            
            if update_qfq_data:
                logger.info("-" * 60)
                logger.info("步骤 5: 更新前复权数据 (qfq_data)")
                logger.info("-" * 60)
                self._update_qfq_data()


            logger.info("等待写入完成...")
            self._wait_write_task_finish()
            logger.info("写入完成")


            logger.info("=" * 60)
            logger.info("历史数据补全流水线执行完成！")
            logger.info("=" * 60)
            
        except KeyboardInterrupt:
            logger.warning("用户中断了流水线执行")
            self._graceful_shutdown()
            raise
        except Exception as e:
            logger.error(f"执行历史数据补全流水线失败: {e}")
            self._graceful_shutdown()
            raise PipelineException(f"执行历史数据补全流水线失败: {e}") from e
        finally:
            # 确保线程池被关闭
            if not self._shutdown_requested:
                self._graceful_shutdown()
    
    def _update_basic_info(self) -> None:
        """
        更新股票基本信息
        """
        try:
            # 1. Extract - 采集数据
            logger.info("采集股票基本信息...")
            raw_data = self.basic_info_collector.collect()
            
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
            self.basic_info_loader.load(clean_data, strategy=BaseLoader.LOAD_STRATEGY_UPSERT)
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
            raw_data = self.trade_calendar_collector.collect(
                start_date=start_date,
                end_date=end_date
            )
            
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
            self.trade_calendar_loader.load(clean_data, strategy=BaseLoader.LOAD_STRATEGY_UPSERT)
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
            start_date = DateHelper.parse_to_date(start_date)
            end_date = DateHelper.parse_to_date(end_date)
            len_date_range = (end_date - start_date).days + 1
            with tqdm(total= len_date_range, desc="采集日K线数据") as pbar:
                for trade_date in pd.date_range(start_date, end_date):
                    # 检查是否收到关闭请求
                    if self._shutdown_requested:
                        logger.warning("收到关闭请求，停止采集数据")
                        break
                    
                    trade_date_str = DateHelper.normalize_to_yyyymmdd(trade_date.strftime('%Y-%m-%d'))
                    raw_data = self.daily_kline_collector.collect(trade_date=trade_date_str)
                    if raw_data is None or raw_data.empty:
                        pbar.update(1)
                        continue 

                    transformed_data = self.daily_kline_transformer.transform(raw_data)
                    if transformed_data is None or transformed_data.empty:
                        pbar.update(1)
                        continue
                    
                    future = self.write_executor.submit(self.daily_kline_loader.load, transformed_data, BaseLoader.LOAD_STRATEGY_APPEND)
                    self.pending_writes.append((future, f"日期: {trade_date_str} daily kline数据写入"))
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
            with tqdm(total=len(ts_code_list), desc="采集复权因子数据") as pbar:
                for ts_code in ts_code_list:
                    # 检查是否收到关闭请求
                    if self._shutdown_requested:
                        logger.warning("收到关闭请求，停止采集数据")
                        break
                    
                    try:
                        raw_data = self.adj_factor_collector.get_single_stock_adj_factor(ts_code)
                        if raw_data is None or raw_data.empty:
                            pbar.update(1)
                            continue
                        transformed_data = self.adj_factor_transformer.transform(raw_data)
                        if transformed_data is None or transformed_data.empty:
                            pbar.update(1)
                            continue
                        future = self.write_executor.submit(self.adj_factor_loader.load, transformed_data, BaseLoader.LOAD_STRATEGY_UPSERT)
                        self.pending_writes.append((future, f"股票: {ts_code} adj factor数据写入"))
                        pbar.update(1)
                    except Exception as e:
                        logger.warning(f"采集股票 {ts_code} 的复权因子失败: {e}")
                        pbar.update(1)
        except Exception as e:
            logger.error(f"更新复权因子失败: {e}")
            raise

    def _update_qfq_data(
        self,
    ) -> None:
        ts_codes = self.basic_info_loader.get_all_ts_codes()
        qfq_calculator = QFQCalculator()
        try:
            with tqdm(total=len(ts_codes), desc="更新前复权数据") as pbar:
                for ts_code in ts_codes:
                    adj_factor_df = self.adj_factor_loader.read(ts_code=ts_code)
                    daily_kline_df = self.daily_kline_loader.read(ts_code=ts_code)
                    if daily_kline_df is None or daily_kline_df.empty:
                        logger.warning(f"股票 {ts_code} 没有日K线数据")
                        continue
                    qfq_calculator_df = qfq_calculator.calculate(daily_kline_df, adj_factor_df)
                    if qfq_calculator_df is None or qfq_calculator_df.empty:
                        logger.warning(f"股票 {ts_code} 没有前复权数据")
                        continue
                    
                    future = self.write_executor.submit(self.daily_kline_loader.load, qfq_calculator_df, BaseLoader.LOAD_STRATEGY_UPSERT)
                    self.pending_writes.append((future, f"股票: {ts_code} qfq数据写入"))
                    pbar.update(1)
        except Exception as e:
            logger.error(f"更新前复权数据失败: {e}")
            raise



    def _wait_write_task_finish(self):
        """等待所有写入任务完成，支持中断"""
        if not self.pending_writes:
            return
        
        with tqdm(total=len(self.pending_writes), desc="等待写入完成", unit="批", leave=False) as pbar:
            try:
                # 使用 as_completed 来等待任务完成，支持中断检查
                for future in as_completed([f for f, _ in self.pending_writes]):
                    # 检查是否收到关闭请求
                    if self._shutdown_requested:
                        logger.warning("收到关闭请求，停止等待写入任务")
                        break
                    
                    # 找到对应的描述
                    desc = next((d for f, d in self.pending_writes if f == future), "未知任务")
                    try:
                        future.result()
                        pbar.update(1)
                    except CancelledError:
                        logger.debug(f"任务已取消: {desc}")
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"写入失败 ({desc}): {e}")
                        pbar.update(1)
            except KeyboardInterrupt:
                logger.warning("用户中断，正在关闭...")
                self._shutdown_requested = True
            except Exception as e:
                logger.debug(f"等待任务时出现异常: {e}")

    def run_single_stock(self, ts_code: str, start_date: str, end_date: str, **kwargs) -> None:
        """
        执行单只股票的历史数据补全流水线
        
        Args:
            ts_code: 股票代码（必需）
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)（必需）
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)（必需）
            **kwargs: 其他参数
                - update_daily_kline: bool, 是否更新 daily_kline（默认 True）
                - update_adj_factor: bool, 是否更新 adj_factor（默认 True）
                - update_qfq_data: bool, 是否更新 qfq_data（默认 True）
        """
        try:
            logger.info("=" * 60)
            logger.info("开始执行单只股票历史数据补全流水线")
            logger.info(f"股票代码: {ts_code}")
            logger.info(f"日期范围: {start_date} ~ {end_date}")
            logger.info("=" * 60)
            
            # 转换为 YYYYMMDD 格式（用于 API 调用）
            start_date_api = DateHelper.normalize_to_yyyymmdd(start_date)
            end_date_api = DateHelper.normalize_to_yyyymmdd(end_date)

            # 获取更新选项
            update_daily_kline = kwargs.get("update_daily_kline", True)
            update_adj_factor = kwargs.get("update_adj_factor", True)
            update_qfq_data = kwargs.get("update_qfq_data", True)
            
            # 1. 更新 daily_kline（日K线数据）
            if update_daily_kline:
                logger.info("-" * 60)
                logger.info("步骤 1: 更新日K线数据 (daily_kline)")
                logger.info("-" * 60)
                self._update_single_stock_daily_kline(ts_code, start_date_api, end_date_api)
            
            # 2. 更新 adj_factor（复权因子）
            if update_adj_factor:
                logger.info("-" * 60)
                logger.info("步骤 2: 更新复权因子 (adj_factor)")
                logger.info("-" * 60)
                self._update_single_stock_adj_factor(ts_code)
            
            # 3. 更新前复权数据
            if update_qfq_data:
                logger.info("-" * 60)
                logger.info("步骤 3: 更新前复权数据 (qfq_data)")
                logger.info("-" * 60)
                self._update_single_stock_qfq_data(ts_code)

            logger.info("等待写入完成...")
            self._wait_write_task_finish()
            logger.info("写入完成")

            logger.info("=" * 60)
            logger.info("单只股票历史数据补全流水线执行完成！")
            logger.info("=" * 60)
            
        except KeyboardInterrupt:
            logger.warning("用户中断了流水线执行")
            self._graceful_shutdown()
            raise
        except Exception as e:
            logger.error(f"执行单只股票历史数据补全流水线失败: {e}")
            self._graceful_shutdown()
            raise PipelineException(f"执行单只股票历史数据补全流水线失败: {e}") from e
        finally:
            # 确保线程池被关闭
            if not self._shutdown_requested:
                self._graceful_shutdown()

    def _update_single_stock_daily_kline(self, ts_code: str, start_date: str, end_date: str) -> None:
        """
        更新单只股票的日K线数据
        
        使用 provider.daily 方法直接获取单只股票的历史数据，比按日期循环更高效
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        try:
            logger.info(f"采集股票 {ts_code} 的日K线数据，日期范围: {start_date} ~ {end_date}...")
            
            # 获取 provider
            provider = self.daily_kline_collector._get_provider()
            
            # 使用 provider.daily 方法直接获取单只股票的历史数据
            raw_data = provider.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到股票 {ts_code} 的日K线数据")
                return
            
            logger.info(f"✓ 采集完成，数据量: {len(raw_data)} 条")
            
            # 转换数据
            logger.info("转换日K线数据...")
            transformed_data = self.daily_kline_transformer.transform(raw_data)
            
            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的股票 {ts_code} 日K线数据为空")
                return
            
            logger.info(f"✓ 转换完成，数据量: {len(transformed_data)} 条")
            
            # 加载数据（使用异步写入）
            logger.info("加载日K线数据到数据库...")
            future = self.write_executor.submit(
                self.daily_kline_loader.load, 
                transformed_data, 
                BaseLoader.LOAD_STRATEGY_APPEND
            )
            self.pending_writes.append((future, f"股票: {ts_code} daily kline数据写入"))
            logger.info(f"✓ 已提交写入任务，共 {len(transformed_data)} 条记录")
            
        except Exception as e:
            logger.error(f"更新股票 {ts_code} 的日K线数据失败: {e}")
            raise

    def _update_single_stock_adj_factor(self, ts_code: str) -> None:
        """
        更新单只股票的复权因子
        
        Args:
            ts_code: 股票代码
        """
        try:
            logger.info(f"采集股票 {ts_code} 的复权因子数据...")
            
            # 使用现有的方法获取单只股票的复权因子
            raw_data = self.adj_factor_collector.get_single_stock_adj_factor(ts_code)
            
            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到股票 {ts_code} 的复权因子数据")
                return
            
            logger.info(f"✓ 采集完成，数据量: {len(raw_data)} 条")
            
            # 转换数据
            logger.info("转换复权因子数据...")
            transformed_data = self.adj_factor_transformer.transform(raw_data)
            
            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的股票 {ts_code} 复权因子数据为空")
                return
            
            logger.info(f"✓ 转换完成，数据量: {len(transformed_data)} 条")
            
            # 加载数据（使用异步写入）
            logger.info("加载复权因子数据到数据库...")
            future = self.write_executor.submit(
                self.adj_factor_loader.load, 
                transformed_data, 
                BaseLoader.LOAD_STRATEGY_UPSERT
            )
            self.pending_writes.append((future, f"股票: {ts_code} adj factor数据写入"))
            logger.info(f"✓ 已提交写入任务，共 {len(transformed_data)} 条记录")
            
        except Exception as e:
            logger.error(f"更新股票 {ts_code} 的复权因子失败: {e}")
            raise

    def _update_single_stock_qfq_data(self, ts_code: str) -> None:
        """
        更新单只股票的前复权数据
        
        Args:
            ts_code: 股票代码
        """
        try:
            logger.info(f"计算股票 {ts_code} 的前复权数据...")
            
            # 读取该股票的复权因子和日K线数据
            adj_factor_df = self.adj_factor_loader.read(ts_code=ts_code)
            daily_kline_df = self.daily_kline_loader.read(ts_code=ts_code)
            
            if daily_kline_df is None or daily_kline_df.empty:
                logger.warning(f"股票 {ts_code} 没有日K线数据，跳过前复权数据计算")
                return
            
            if adj_factor_df is None or adj_factor_df.empty:
                logger.warning(f"股票 {ts_code} 没有复权因子数据，跳过前复权数据计算")
                return
            
            # 计算前复权数据
            qfq_calculator = QFQCalculator()
            qfq_calculator_df = qfq_calculator.calculate(daily_kline_df, adj_factor_df)
            
            if qfq_calculator_df is None or qfq_calculator_df.empty:
                logger.warning(f"股票 {ts_code} 前复权数据计算后为空")
                return
            
            logger.info(f"✓ 计算完成，数据量: {len(qfq_calculator_df)} 条")
            
            # 加载数据（使用异步写入）
            logger.info("加载前复权数据到数据库...")
            future = self.write_executor.submit(
                self.daily_kline_loader.load, 
                qfq_calculator_df, 
                BaseLoader.LOAD_STRATEGY_UPSERT
            )
            self.pending_writes.append((future, f"股票: {ts_code} qfq数据写入"))
            logger.info(f"✓ 已提交写入任务，共 {len(qfq_calculator_df)} 条记录")
            
        except Exception as e:
            logger.error(f"更新股票 {ts_code} 的前复权数据失败: {e}")
            raise