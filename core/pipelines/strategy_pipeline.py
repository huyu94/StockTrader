"""
策略流水线

用于聚合历史k线和实时k线数据，运行策略筛选股票
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from loguru import logger

from core.pipelines.base import BasePipeline
from core.common.exceptions import PipelineException
from core.strategies.base import BaseStrategy
from core.calculators.aggregator import Aggregator
from utils.date_helper import DateHelper
from core.pipelines.strategy_worker import process_single_stock


class StrategyPipeline(BasePipeline):
    """
    策略流水线
    
    功能：
    1. 读取历史日K线数据（已包含前复权价格字段，按股票分离，支持多线程加速）
    2. 读取当天实时k线数据（按股票分离，支持多线程加速）
    3. 使用聚合器将实时k线数据聚合为日K线
    4. 对每只股票分别拼接历史k线和当天实时k线（保持数据分离，不全局拼接）
    5. 对每只股票调用策略类，计算指标并筛选（支持多进程并行处理）
    6. 返回筛选结果并保存到文件
    
    优化特性：
    - 数据按股票分离存储，避免大内存占用
    - 读取数据使用多线程（I/O密集型任务）
    - 策略计算使用多进程（CPU密集型任务）
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化策略流水线
        
        Args:
            config: 配置字典，包含：
                - output_dir: 输出目录（默认 "output"）
                - output_format: 输出格式（"csv" 或 "json"，默认 "csv"）
                - use_multiprocessing: 是否使用多进程并行处理策略计算（默认 False）
                - max_workers: 最大进程数（默认 None，使用CPU核心数）
                - use_multithreading: 是否使用多线程读取数据（默认 True）
                - thread_workers: 最大线程数（默认 None，使用 min(20, CPU核心数*4)）
        """
        super().__init__(config)
        self.aggregator = Aggregator()
        
        # 输出配置
        self.output_dir = Path(self.config.get("output_dir", "output"))
        self.output_format = self.config.get("output_format", "csv")
        
        # 多进程配置
        self.use_multiprocessing = self.config.get("use_multiprocessing", False)
        self.max_workers = self.config.get("max_workers", None)
        if self.max_workers is None and self.use_multiprocessing:
            self.max_workers = multiprocessing.cpu_count()
        
        # 多线程配置（用于I/O密集型操作，如读取数据）
        self.use_multithreading = self.config.get("use_multithreading", True)  # 默认启用
        self.thread_workers = self.config.get("thread_workers", None)
        if self.thread_workers is None:
            # I/O密集型任务可以使用更多线程
            self.thread_workers = min(20, multiprocessing.cpu_count() * 4)
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def run(
        self,
        strategy: BaseStrategy,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_date: Optional[str] = None,
        **kwargs
    ) -> Union[List[str], pd.DataFrame]:
        """
        执行策略流水线
        
        Args:
            strategy: 策略实例
            ts_codes: 股票代码列表（可选，如果不提供则处理所有股票）
            start_date: 历史数据开始日期 (YYYY-MM-DD)（可选）
            end_date: 历史数据结束日期 (YYYY-MM-DD)（可选，默认为trade_date的前一天）
            trade_date: 当天交易日期 (YYYY-MM-DD)（可选，默认为今天）
            **kwargs: 其他参数
        
        Returns:
            Union[List[str], pd.DataFrame]: 策略筛选结果
        """
        try:
            logger.info("=" * 60)
            logger.info(f"开始执行策略流水线: {strategy.name}")
            logger.info("=" * 60)
            
            # 确定交易日期
            if trade_date is None:
                trade_date = DateHelper.today()
            trade_date = DateHelper.normalize_to_yyyy_mm_dd(trade_date)
            
            # 确定历史数据结束日期（默认为trade_date的前一天）
            if end_date is None:
                # 简单实现：使用trade_date的前一天
                trade_date_obj = DateHelper.parse_to_date(trade_date)
                end_date_obj = trade_date_obj - timedelta(days=1)
                end_date = DateHelper.parse_to_str(end_date_obj)
            end_date = DateHelper.normalize_to_yyyy_mm_dd(end_date)
            
            logger.info(f"交易日期: {trade_date}")
            logger.info(f"历史数据日期范围: {start_date or '全部'} ~ {end_date}")
            
            # 1. 读取历史日K线数据（已包含前复权价格字段，按股票分离）
            logger.info("\n步骤 1: 读取历史日K线数据...")
            historical_dict = self._read_historical_kline(
                ts_codes=ts_codes,
                start_date=start_date,
                end_date=end_date
            )
            
            if not historical_dict:
                logger.warning("历史日K线数据为空")
                return [] if isinstance(strategy.filter_stocks(pd.DataFrame()), list) else pd.DataFrame()
            
            total_historical_rows = sum(len(df) for df in historical_dict.values())
            logger.info(f"✓ 读取到 {len(historical_dict)} 只股票，共 {total_historical_rows} 条历史日K线数据")
            
            # 2. 读取当天实时k线数据（按股票分离）
            logger.info("\n步骤 2: 读取当天实时k线数据...")
            intraday_dict = self._read_intraday_kline(
                ts_codes=ts_codes,
                trade_date=trade_date
            )
            
            total_intraday_rows = sum(len(df) for df in intraday_dict.values())
            if not intraday_dict:
                logger.warning("当天实时k线数据为空，仅使用历史数据")
            else:
                logger.info(f"✓ 读取到 {len(intraday_dict)} 只股票，共 {total_intraday_rows} 条实时k线数据")
            
            # 3. 对每只股票分别处理：聚合实时数据并合并历史数据
            logger.info("\n步骤 3: 处理每只股票的数据（聚合实时数据并合并历史数据）...")
            stock_data_dict = {}  # key: ts_code, value: 合并后的DataFrame
            
            # 获取所有股票代码（历史数据和实时数据的并集）
            all_stocks = set(historical_dict.keys()) | set(intraday_dict.keys())
            
            for ts_code in all_stocks:
                # 获取历史数据
                hist_df = historical_dict.get(ts_code, pd.DataFrame())
                
                # 获取实时数据并聚合
                intra_df = intraday_dict.get(ts_code, pd.DataFrame())
                if not intra_df.empty:
                    # 聚合实时k线数据为日K线
                    daily_from_intraday = self.aggregator.aggregate_to_daily(intra_df)
                    
                    if not daily_from_intraday.empty:
                        # 合并历史数据和当天数据
                        merged_df = self._merge_historical_and_realtime_single_stock(
                            historical_df=hist_df,
                            daily_from_intraday=daily_from_intraday,
                            trade_date=trade_date
                        )
                        stock_data_dict[ts_code] = merged_df
                    else:
                        # 聚合失败，仅使用历史数据
                        if not hist_df.empty:
                            stock_data_dict[ts_code] = self._prepare_final_dataframe(hist_df)
                else:
                    # 没有实时数据，仅使用历史数据
                    if not hist_df.empty:
                        stock_data_dict[ts_code] = self._prepare_final_dataframe(hist_df)
            
            logger.info(f"✓ 数据处理完成，共 {len(stock_data_dict)} 只股票")
            
            # 4. 对每只股票调用策略类，计算指标并筛选
            logger.info("\n步骤 4: 运行策略筛选股票...")
            
            # 根据配置决定是否使用多进程
            if self.use_multiprocessing:
                result = self._run_strategy_multiprocess_separated(strategy, stock_data_dict)
            else:
                # 串行处理：将所有股票数据拼接后运行策略
                all_data_list = list(stock_data_dict.values())
                if all_data_list:
                    final_df = pd.concat(all_data_list, ignore_index=True)
                    result = strategy.run(final_df)
                else:
                    result = [] if isinstance(strategy.filter_stocks(pd.DataFrame()), list) else pd.DataFrame()
            
            # 6. 保存结果到文件
            logger.info("\n步骤 6: 保存筛选结果到文件...")
            self._save_result(result, strategy.name, trade_date)
            
            logger.info("=" * 60)
            logger.info("策略流水线执行完成！")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"执行策略流水线失败: {e}")
            raise PipelineException(f"执行策略流水线失败: {e}") from e
    
    def _read_historical_kline(
        self,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        读取历史日K线数据（按股票代码分离，不拼接）
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            Dict[str, pd.DataFrame]: 按股票代码分组的历史日K线数据字典
                key: 股票代码, value: 该股票的历史K线数据
        """
        def read_single_stock(ts_code: str) -> tuple:
            """读取单只股票的数据（用于多线程）"""
            try:
                df = self.daily_kline_loader.read(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                return (ts_code, df)
            except Exception as e:
                logger.error(f"读取股票 {ts_code} 的历史数据失败: {e}")
                return (ts_code, pd.DataFrame())
        
        if ts_codes is None or len(ts_codes) == 0:
            # 读取所有股票的数据
            # 先获取所有股票代码
            all_df = self.daily_kline_loader.read(
                ts_code=None,
                start_date=start_date,
                end_date=end_date
            )
            
            if all_df.empty:
                return {}
            
            # 按股票代码分组，返回字典
            result_dict = {}
            for ts_code, group_df in all_df.groupby('ts_code'):
                result_dict[ts_code] = group_df.reset_index(drop=True)
            
            return result_dict
        else:
            # 读取指定股票的数据，使用多线程加速
            result_dict = {}
            
            if self.use_multithreading and len(ts_codes) > 1:
                # 使用多线程并行读取
                logger.info(f"使用多线程读取 {len(ts_codes)} 只股票的历史数据，线程数: {self.thread_workers}")
                
                with ThreadPoolExecutor(max_workers=self.thread_workers) as executor:
                    future_to_ts_code = {
                        executor.submit(read_single_stock, ts_code): ts_code
                        for ts_code in ts_codes
                    }
                    
                    completed = 0
                    for future in as_completed(future_to_ts_code):
                        ts_code = future_to_ts_code[future]
                        try:
                            code, df = future.result()
                            if not df.empty:
                                result_dict[code] = df
                            completed += 1
                            if completed % 50 == 0:
                                logger.info(f"已读取 {completed}/{len(ts_codes)} 只股票的历史数据...")
                        except Exception as e:
                            logger.error(f"读取股票 {ts_code} 的历史数据时出错: {e}")
                            completed += 1
                
                logger.info(f"历史数据读取完成，共读取 {len(result_dict)} 只股票的数据")
            else:
                # 串行读取
                for ts_code in ts_codes:
                    df = self.daily_kline_loader.read(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if not df.empty:
                        result_dict[ts_code] = df
            
            return result_dict
    
    def _read_intraday_kline(
        self,
        ts_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        读取当天实时k线数据（按股票代码分离，不拼接）
        
        Args:
            ts_codes: 股票代码列表
            trade_date: 交易日期
        
        Returns:
            Dict[str, pd.DataFrame]: 按股票代码分组的实时k线数据字典
                key: 股票代码, value: 该股票的实时k线数据
        """
        def read_single_stock(ts_code: str) -> tuple:
            """读取单只股票的实时数据（用于多线程）"""
            try:
                df = self.intraday_kline_loader.read(
                    ts_code=ts_code,
                    trade_date=trade_date
                )
                return (ts_code, df)
            except Exception as e:
                logger.error(f"读取股票 {ts_code} 的实时数据失败: {e}")
                return (ts_code, pd.DataFrame())
        
        if ts_codes is None or len(ts_codes) == 0:
            # 读取所有股票的数据
            all_df = self.intraday_kline_loader.read(
                ts_code=None,
                trade_date=trade_date
            )
            
            if all_df.empty:
                return {}
            
            # 按股票代码分组，返回字典
            result_dict = {}
            for ts_code, group_df in all_df.groupby('ts_code'):
                result_dict[ts_code] = group_df.reset_index(drop=True)
            
            return result_dict
        else:
            # 读取指定股票的数据，使用多线程加速
            result_dict = {}
            
            if self.use_multithreading and len(ts_codes) > 1:
                # 使用多线程并行读取
                logger.info(f"使用多线程读取 {len(ts_codes)} 只股票的实时数据，线程数: {self.thread_workers}")
                
                with ThreadPoolExecutor(max_workers=self.thread_workers) as executor:
                    future_to_ts_code = {
                        executor.submit(read_single_stock, ts_code): ts_code
                        for ts_code in ts_codes
                    }
                    
                    completed = 0
                    for future in as_completed(future_to_ts_code):
                        ts_code = future_to_ts_code[future]
                        try:
                            code, df = future.result()
                            if not df.empty:
                                result_dict[code] = df
                            completed += 1
                            if completed % 50 == 0:
                                logger.info(f"已读取 {completed}/{len(ts_codes)} 只股票的实时数据...")
                        except Exception as e:
                            logger.error(f"读取股票 {ts_code} 的实时数据时出错: {e}")
                            completed += 1
                
                logger.info(f"实时数据读取完成，共读取 {len(result_dict)} 只股票的数据")
            else:
                # 串行读取
                for ts_code in ts_codes:
                    df = self.intraday_kline_loader.read(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                    if not df.empty:
                        result_dict[ts_code] = df
            
            return result_dict
    
    def _prepare_final_dataframe(self, historical_df: pd.DataFrame) -> pd.DataFrame:
        """
        准备最终DataFrame（仅使用历史数据时）
        
        将历史数据的前复权价格字段转换为标准列名
        
        Args:
            historical_df: 历史日K线数据
        
        Returns:
            pd.DataFrame: 准备好的DataFrame
        """
        df = historical_df.copy()
        
        # 如果存在前复权价格字段，使用它们替换标准价格字段
        if 'close_qfq' in df.columns:
            df['close'] = df['close_qfq']
        if 'open_qfq' in df.columns:
            df['open'] = df['open_qfq']
        if 'high_qfq' in df.columns:
            df['high'] = df['high_qfq']
        if 'low_qfq' in df.columns:
            df['low'] = df['low_qfq']
        
        # 确保必需的列存在
        required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"数据缺少必需的列: {missing_columns}")
        
        return df
    
    def _merge_historical_and_realtime_single_stock(
        self,
        historical_df: pd.DataFrame,
        daily_from_intraday: pd.DataFrame,
        trade_date: str
    ) -> pd.DataFrame:
        """
        拼接单只股票的历史k线和当天实时k线
        
        Args:
            historical_df: 单只股票的历史日K线数据（包含前复权价格字段）
            daily_from_intraday: 单只股票的当天实时k线聚合后的日K线数据
            trade_date: 交易日期
        
        Returns:
            pd.DataFrame: 拼接后的DataFrame
        """
        # 如果历史数据为空，直接返回当天数据
        if historical_df.empty:
            return daily_from_intraday.copy()
        
        # 如果当天数据为空，直接返回历史数据
        if daily_from_intraday.empty:
            return self._prepare_final_dataframe(historical_df)
        
        # 准备历史数据：使用前复权价格字段
        historical_prepared = self._prepare_final_dataframe(historical_df)
        
        # 准备当天数据：直接使用聚合后的数据
        daily_prepared = daily_from_intraday.copy()
        
        # 确保当天数据包含所有必需的列
        required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        for col in required_columns:
            if col not in daily_prepared.columns:
                if col == 'vol' and 'volume' in daily_prepared.columns:
                    daily_prepared['vol'] = daily_prepared['volume']
                elif col == 'amount' and 'amount' in daily_prepared.columns:
                    pass  # amount已经存在
                else:
                    logger.warning(f"当天数据缺少列: {col}")
        
        # 确保trade_date是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(daily_prepared['trade_date']):
            daily_prepared['trade_date'] = pd.to_datetime(daily_prepared['trade_date'], errors='coerce')
        
        # 过滤掉历史数据中与当天日期相同的数据（避免重复）
        trade_date_obj = pd.to_datetime(trade_date)
        historical_prepared = historical_prepared[
            historical_prepared['trade_date'] < trade_date_obj
        ]
        
        # 拼接数据
        # 选择需要的列
        common_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        available_columns = [col for col in common_columns if col in historical_prepared.columns and col in daily_prepared.columns]
        
        historical_selected = historical_prepared[available_columns].copy()
        daily_selected = daily_prepared[available_columns].copy()
        
        # 合并
        merged_df = pd.concat([historical_selected, daily_selected], ignore_index=True)
        
        # 按日期排序
        merged_df = merged_df.sort_values('trade_date').reset_index(drop=True)
        
        return merged_df
    
    def _merge_historical_and_realtime(
        self,
        historical_df: pd.DataFrame,
        daily_from_intraday: pd.DataFrame,
        trade_date: str
    ) -> pd.DataFrame:
        """
        拼接历史k线和当天实时k线
        
        Args:
            historical_df: 历史日K线数据（包含前复权价格字段）
            daily_from_intraday: 当天实时k线聚合后的日K线数据
            trade_date: 交易日期
        
        Returns:
            pd.DataFrame: 拼接后的DataFrame
        """
        # 准备历史数据：使用前复权价格字段
        historical_prepared = self._prepare_final_dataframe(historical_df)
        
        # 准备当天数据：直接使用聚合后的数据
        # 确保列名一致
        daily_prepared = daily_from_intraday.copy()
        
        # 确保当天数据包含所有必需的列
        required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        for col in required_columns:
            if col not in daily_prepared.columns:
                if col == 'vol' and 'volume' in daily_prepared.columns:
                    daily_prepared['vol'] = daily_prepared['volume']
                elif col == 'amount' and 'amount' in daily_prepared.columns:
                    pass  # amount已经存在
                else:
                    logger.warning(f"当天数据缺少列: {col}")
        
        # 确保trade_date是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(daily_prepared['trade_date']):
            daily_prepared['trade_date'] = pd.to_datetime(daily_prepared['trade_date'], errors='coerce')
        
        # 过滤掉历史数据中与当天日期相同的数据（避免重复）
        trade_date_obj = pd.to_datetime(trade_date)
        historical_prepared = historical_prepared[
            historical_prepared['trade_date'] < trade_date_obj
        ]
        
        # 拼接数据
        # 选择需要的列
        common_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        available_columns = [col for col in common_columns if col in historical_prepared.columns and col in daily_prepared.columns]
        
        historical_selected = historical_prepared[available_columns].copy()
        daily_selected = daily_prepared[available_columns].copy()
        
        # 合并
        merged_df = pd.concat([historical_selected, daily_selected], ignore_index=True)
        
        # 按股票代码和日期排序
        merged_df = merged_df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
        
        return merged_df
    
    def _save_result(
        self,
        result: Union[List[str], pd.DataFrame],
        strategy_name: str,
        trade_date: str
    ) -> None:
        """
        保存筛选结果到文件
        
        Args:
            result: 筛选结果（股票代码列表或DataFrame）
            strategy_name: 策略名称
            trade_date: 交易日期
        """
        try:
            # 生成文件名
            safe_strategy_name = strategy_name.replace(' ', '_').replace('/', '_')
            timestamp = trade_date.replace('-', '')
            
            if isinstance(result, list):
                # 如果是列表，转换为DataFrame
                result_df = pd.DataFrame({
                    'ts_code': result,
                    'strategy': strategy_name,
                    'trade_date': trade_date
                })
            else:
                # 如果是DataFrame，确保包含策略名称和日期
                result_df = result.copy()
                if 'strategy' not in result_df.columns:
                    result_df['strategy'] = strategy_name
                if 'trade_date' not in result_df.columns:
                    result_df['trade_date'] = trade_date
            
            # 保存文件
            if self.output_format.lower() == 'json':
                output_file = self.output_dir / f"{safe_strategy_name}_{timestamp}.json"
                result_df.to_json(output_file, orient='records', force_ascii=False, indent=2)
                logger.info(f"✓ 结果已保存到: {output_file}")
            else:
                # 默认保存为CSV
                output_file = self.output_dir / f"{safe_strategy_name}_{timestamp}.csv"
                result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                logger.info(f"✓ 结果已保存到: {output_file}")
            
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            # 不抛出异常，只记录错误
    
    def _run_strategy_multiprocess_separated(
        self,
        strategy: BaseStrategy,
        stock_data_dict: Dict[str, pd.DataFrame]
    ) -> Union[List[str], pd.DataFrame]:
        """
        使用多进程并行运行策略（使用分离的股票数据）
        
        Args:
            strategy: 策略实例
            stock_data_dict: 按股票代码分组的数据字典
        
        Returns:
            Union[List[str], pd.DataFrame]: 策略筛选结果
        """
        if not stock_data_dict:
            logger.warning("数据为空，无法运行策略")
            return [] if isinstance(strategy.filter_stocks(pd.DataFrame()), list) else pd.DataFrame()
        
        num_stocks = len(stock_data_dict)
        logger.info(f"使用多进程处理 {num_stocks} 只股票，进程数: {self.max_workers}")
        
        # 获取策略类和参数（用于在子进程中重建策略实例）
        strategy_class = strategy.__class__
        strategy_params = self._extract_strategy_params(strategy)
        
        # 准备任务参数：直接使用分离的数据
        tasks = [
            (ts_code, stock_df, strategy_class, strategy_params)
            for ts_code, stock_df in stock_data_dict.items()
        ]
        
        # 使用进程池并行处理
        results = []
        completed = 0
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_ts_code = {
                executor.submit(process_single_stock, ts_code, stock_df, strategy_class, strategy_params): ts_code
                for ts_code, stock_df, strategy_class, strategy_params in tasks
            }
            
            # 收集结果
            for future in as_completed(future_to_ts_code):
                ts_code = future_to_ts_code[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                    completed += 1
                    if completed % 50 == 0:
                        logger.info(f"已处理 {completed}/{num_stocks} 只股票...")
                except Exception as e:
                    logger.error(f"处理股票 {ts_code} 时出错: {e}")
                    completed += 1
        
        logger.info(f"多进程处理完成，共处理 {completed} 只股票")
        
        # 合并结果
        if not results:
            return [] if isinstance(strategy.filter_stocks(pd.DataFrame()), list) else pd.DataFrame()
        
        # 判断结果类型并合并
        if isinstance(results[0], list):
            # 如果结果是列表，合并所有列表
            merged_result = []
            for r in results:
                if isinstance(r, list):
                    merged_result.extend(r)
            return merged_result
        elif isinstance(results[0], pd.DataFrame):
            # 如果结果是DataFrame，合并所有DataFrame
            return pd.concat(results, ignore_index=True)
        else:
            # 其他情况，返回空结果
            return [] if isinstance(strategy.filter_stocks(pd.DataFrame()), list) else pd.DataFrame()
    
    def _extract_strategy_params(self, strategy: BaseStrategy) -> Dict[str, Any]:
        """
        提取策略的初始化参数（用于在子进程中重建策略实例）
        
        Args:
            strategy: 策略实例
        
        Returns:
            Dict[str, Any]: 策略参数字典
        """
        import inspect
        
        params = {}
        
        # 获取策略类的__init__方法的参数
        sig = inspect.signature(strategy.__class__.__init__)
        
        # 需要排除的参数（通常是内部对象，不需要传递）
        exclude_params = {'self', 'name'}  # name会在子进程中重新设置
        
        for param_name in sig.parameters:
            if param_name in exclude_params:
                continue
            
            # 尝试从策略实例中获取参数值
            if hasattr(strategy, param_name):
                value = getattr(strategy, param_name)
                
                # 只序列化基本类型和可序列化的对象
                # 排除对象实例（如indicator_calculator等）
                if isinstance(value, (str, int, float, bool, type(None))):
                    params[param_name] = value
                elif isinstance(value, list):
                    # 检查列表中的元素是否可序列化
                    if all(isinstance(item, (str, int, float, bool, type(None))) for item in value):
                        params[param_name] = value
                elif isinstance(value, tuple):
                    # 将tuple转换为list以便序列化
                    if all(isinstance(item, (str, int, float, bool, type(None))) for item in value):
                        params[param_name] = list(value)
                elif isinstance(value, dict):
                    # 检查字典的值是否可序列化
                    if all(isinstance(k, (str, int, float, bool, type(None))) and 
                           isinstance(v, (str, int, float, bool, type(None), list, dict)) 
                           for k, v in value.items()):
                        params[param_name] = value
        
        return params

