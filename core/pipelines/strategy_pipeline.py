"""
策略流水线

用于聚合历史k线和实时k线数据，运行策略筛选股票
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pathlib import Path
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from loguru import logger
from tqdm import tqdm

from core.pipelines.base import BasePipeline
from core.common.exceptions import PipelineException
from core.strategies.base import BaseStrategy
from core.loaders.base import BaseLoader
from utils.date_helper import DateHelper
from utils.message_robot import MessageRobot
from core.pipelines.strategy_worker import process_single_stock_complete, process_batch_stocks_complete


class StrategyPipeline(BasePipeline):
    """
    策略流水线
    
    功能：
    1. 更新实时K线数据（可选，默认启用）
    2. 每只股票一个进程，在子进程中完成完整流程：
       - 读取历史日K线数据（已包含前复权价格字段）
       - 读取当天实时k线数据
       - 使用聚合器将实时k线数据聚合为日K线
       - 拼接历史k线和当天实时k线
       - 计算指标并筛选股票
    3. 返回筛选结果并保存到文件
    
    优化特性：
    - 每个进程处理一批股票，完成从数据读取到策略筛选的完整流程
    - 每个进程只创建一次策略实例，在该进程中复用，减少重复初始化
    - 真正的端到端并行处理，包括I/O和计算
    - 数据完全隔离，避免大内存占用
    - 默认启用多进程并行处理
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
                - batch_size: 每个进程处理的股票批次大小（默认 None，自动计算）
        """
        super().__init__(config)
        
        # 输出配置
        self.output_dir = Path(self.config.get("output_dir", "output"))
        self.output_format = self.config.get("output_format", "csv")
        
        # 多进程配置（默认启用，因为新架构完全基于多进程）
        self.use_multiprocessing = self.config.get("use_multiprocessing", True)  # 默认启用
        self.max_workers = self.config.get("max_workers", None)
        if self.max_workers is None:
            self.max_workers = multiprocessing.cpu_count()
        
        # 多线程配置（用于I/O密集型操作，如读取数据）
        self.use_multithreading = self.config.get("use_multithreading", True)  # 默认启用
        self.thread_workers = self.config.get("thread_workers", None)
        if self.thread_workers is None:
            # I/O密集型任务可以使用更多线程
            self.thread_workers = min(20, multiprocessing.cpu_count() * 4)
        
        # 批次大小配置（用于批量处理股票，减少策略实例创建次数）
        self.batch_size = self.config.get("batch_size", None)
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def run(
        self,
        strategies_config: List[Dict[str, Any]],
        ts_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        update_real_time_data: bool = True,
        send_to_robots: bool = True,
        **kwargs
    ) -> Union[List[str], pd.DataFrame, Dict[str, Union[List[str], pd.DataFrame]]]:
        """
        执行策略流水线
        
        Args:
            strategies_config: 策略配置列表，每个配置包含：
                - strategy_class: 策略类
                - strategy_params: 策略初始化参数
                - start_date_days: 历史数据开始日期（天数）
                - name: 策略名称（可选）
            ts_codes: 股票代码列表（可选，如果不提供则处理所有股票）
            trade_date: 当天交易日期 (YYYY-MM-DD)（可选，默认为今天）
            update_real_time_data: 是否先更新实时K线数据（默认 True）
            send_to_robots: 是否发送消息到机器人（默认 True）
            **kwargs: 其他参数
        
        Returns:
            Union[List[str], pd.DataFrame, Dict[str, Union[List[str], pd.DataFrame]]]: 
                - 单个策略时：返回策略筛选结果
                - 多个策略时：返回结果字典 {strategy_name: result}
        """
        try:
            if not strategies_config:
                raise ValueError("strategies_config 不能为空")
            
            # 确定交易日期
            if trade_date is None:
                trade_date = DateHelper.today()
            trade_date = DateHelper.normalize_to_yyyy_mm_dd(trade_date)
            
            # 0. 统一更新实时K线数据（只执行一次）
            if update_real_time_data:
                logger.info("-" * 60)
                logger.info("步骤 0: 统一更新实时K线数据 (intraday_kline)")
                logger.info("-" * 60)
                try:
                    self._update_real_time_data(trade_date)
                except Exception as e:
                    logger.warning(f"更新实时K线数据失败，日期:{trade_date}，错误:{e}，继续执行策略筛选")
                    # 实时数据更新失败不应该阻止策略运行，只记录警告
            
            # 1. 获取股票代码列表
            if ts_codes is None or len(ts_codes) == 0:
                try:
                    ts_codes = self.basic_info_loader.get_all_ts_codes()
                except Exception as e:
                    logger.error(f"获取股票代码列表失败: {e}")
                    # 返回空结果字典
                    empty_results = {}
                    for config in strategies_config:
                        strategy_name = config.get('name', 'Unknown')
                        try:
                            strategy = config['strategy_class'](**config.get('strategy_params', {}))
                            empty_result = strategy.filter_stocks(pd.DataFrame())
                            empty_results[strategy_name] = empty_result
                        except:
                            empty_results[strategy_name] = []
                    # 如果只有一个策略，返回单个结果；否则返回结果字典
                    if len(strategies_config) == 1:
                        return list(empty_results.values())[0]
                    return empty_results
            
            if not ts_codes:
                logger.warning("股票代码列表为空")
                empty_results = {}
                for config in strategies_config:
                    strategy_name = config.get('name', 'Unknown')
                    try:
                        strategy = config['strategy_class'](**config.get('strategy_params', {}))
                        empty_result = strategy.filter_stocks(pd.DataFrame())
                        empty_results[strategy_name] = empty_result
                    except:
                        empty_results[strategy_name] = []
                # 如果只有一个策略，返回单个结果；否则返回结果字典
                if len(strategies_config) == 1:
                    return list(empty_results.values())[0]
                return empty_results
            
            # 2. 使用线程池并行运行所有策略
            results_dict = {}
            num_strategies = len(strategies_config)
            
            logger.info(f"开始并行运行 {num_strategies} 个策略")
            
            with ThreadPoolExecutor(max_workers=num_strategies) as executor:
                # 提交所有策略任务
                future_to_strategy = {
                    executor.submit(self._run_single_strategy, config, ts_codes, trade_date, send_to_robots): config.get('name', 'Unknown')
                    for config in strategies_config
                }
                
                # 收集结果
                for future in as_completed(future_to_strategy):
                    strategy_name = future_to_strategy[future]
                    try:
                        name, result = future.result()
                        results_dict[name] = result
                        logger.info(f"策略 {name} 执行完成")
                    except Exception as e:
                        logger.error(f"策略 {strategy_name} 执行异常: {e}")
                        # 返回空结果
                        try:
                            # 找到对应的策略配置
                            config = next((c for c in strategies_config if c.get('name', 'Unknown') == strategy_name), strategies_config[0])
                            strategy = config['strategy_class'](**config.get('strategy_params', {}))
                            empty_result = strategy.filter_stocks(pd.DataFrame())
                        except:
                            empty_result = []
                        results_dict[strategy_name] = empty_result
            
            logger.info(f"所有策略执行完成，共 {len(results_dict)} 个策略")
            
            # 如果只有一个策略，返回单个结果；否则返回结果字典
            if len(strategies_config) == 1:
                return list(results_dict.values())[0]
            return results_dict
            
        except Exception as e:
            logger.error(f"执行策略流水线失败: {e}")
            raise PipelineException(f"执行策略流水线失败: {e}") from e
    
    def _run_single_strategy(
        self,
        strategy_config: Dict[str, Any],
        ts_codes: List[str],
        trade_date: str,
        send_to_robots: bool = True
    ) -> tuple:
        """
        执行单个策略
        
        Args:
            strategy_config: 策略配置字典
            ts_codes: 股票代码列表
            trade_date: 交易日期
            send_to_robots: 是否发送消息到机器人
        
        Returns:
            tuple: (strategy_name, result) 策略名称和筛选结果
        """
        try:
            strategy_class = strategy_config['strategy_class']
            strategy_params = strategy_config.get('strategy_params', {})
            start_date_days = strategy_config.get('start_date_days', 365)
            strategy_name = strategy_config.get('name', strategy_class.__name__)
            
            # 创建策略实例
            strategy = strategy_class(**strategy_params)
            
            # 计算开始日期
            start_date = DateHelper.days_ago(start_date_days)
            end_date = trade_date
            
            # 运行策略（不更新实时数据，因为已经统一更新了）
            result = self._run_complete_pipeline_multiprocess(
                strategy=strategy,
                ts_codes=ts_codes,
                start_date=start_date,
                end_date=end_date,
                trade_date=trade_date
            )
            
            # 保存结果到文件
            self._save_result(result, strategy_name, trade_date, send_to_robots=send_to_robots)
            
            return (strategy_name, result)
        except Exception as e:
            logger.error(f"策略 {strategy_config.get('name', 'Unknown')} 执行失败: {e}")
            strategy_name = strategy_config.get('name', strategy_config.get('strategy_class', type).__name__)
            # 返回空结果
            try:
                strategy = strategy_config['strategy_class'](**strategy_config.get('strategy_params', {}))
                empty_result = strategy.filter_stocks(pd.DataFrame())
            except:
                empty_result = []
            return (strategy_name, empty_result)
    
    def _save_result(
        self,
        result: Union[List[str], pd.DataFrame],
        strategy_name: str,
        trade_date: str,
        send_to_robots: bool = True
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
            
            # 添加股票名称
            if not result_df.empty and 'ts_code' in result_df.columns:
                try:
                    # 获取所有股票代码
                    ts_codes = result_df['ts_code'].unique().tolist()
                    # 从数据库读取股票基本信息
                    basic_info_df = self.basic_info_loader.read(ts_codes=ts_codes)
                    
                    if not basic_info_df.empty and 'name' in basic_info_df.columns:
                        # 合并股票名称
                        result_df = result_df.merge(
                            basic_info_df[['ts_code', 'name']],
                            on='ts_code',
                            how='left'
                        )
                        # 调整列顺序，将name放在ts_code后面
                        cols = result_df.columns.tolist()
                        if 'name' in cols and 'ts_code' in cols:
                            cols.remove('name')
                            ts_code_idx = cols.index('ts_code')
                            cols.insert(ts_code_idx + 1, 'name')
                            result_df = result_df[cols]
                except Exception as e:
                    logger.warning(f"获取股票名称失败: {e}，将不包含股票名称")
            
            # 保存文件
            if self.output_format.lower() == 'json':
                output_file = self.output_dir / f"{safe_strategy_name}_{timestamp}.json"
                result_df.to_json(output_file, orient='records', force_ascii=False, indent=2)
            else:
                # 默认保存为CSV
                output_file = self.output_dir / f"{safe_strategy_name}_{timestamp}.csv"
                result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            # 发送消息到机器人
            if send_to_robots:
                self._send_stock_message(result_df, strategy_name, trade_date)
            else:
                logger.info("不发送消息到机器人")
            
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            # 不抛出异常，只记录错误
    
    def _send_stock_message(
        self,
        result_df: pd.DataFrame,
        strategy_name: str,
        trade_date: str
    ) -> None:
        """
        发送选中的股票信息到机器人
        
        Args:
            result_df: 筛选结果DataFrame（包含ts_code和name列）
            strategy_name: 策略名称
            trade_date: 交易日期
        """
        try:
            # 检查是否有结果
            if result_df.empty:
                logger.info("筛选结果为空，不发送消息")
                return
            
            # 检查必要的列
            if 'ts_code' not in result_df.columns:
                logger.warning("结果中缺少ts_code列，无法发送消息")
                return
            
            # 提取股票代码和名称
            stocks = []
            for _, row in result_df.iterrows():
                ts_code = row['ts_code']
                name = row.get('name', '未知')
                stocks.append(f"{name}({ts_code})")
            
            # 构建消息内容
            stock_count = len(stocks)
            message_lines = [
                f"【{strategy_name}】策略筛选结果",
                f"交易日期: {trade_date}",
                f"选中股票数量: {stock_count}",
                "",
                "股票列表:"
            ]
            
            # 添加股票信息（每行一个）
            for stock in stocks:
                message_lines.append(stock)
            
            message = "\n".join(message_lines)
            
            # 发送消息
            message_robot = MessageRobot()
            message_robot.send_message(message)
            logger.info(f"已发送 {stock_count} 只股票信息到机器人")
            
        except Exception as e:
            logger.warning(f"发送消息到机器人失败: {e}，继续执行")
            # 不抛出异常，只记录警告，避免影响主流程
    
    def _run_complete_pipeline_multiprocess(
        self,
        strategy: BaseStrategy,
        ts_codes: List[str],
        start_date: Optional[str],
        end_date: Optional[str],
        trade_date: str
    ) -> Union[List[str], pd.DataFrame]:
        """
        使用多进程并行运行完整流程（每个进程处理一批股票，完成从数据读取到策略筛选的完整流程）
        
        优化：每个进程只创建一次策略实例，然后在该进程中循环处理多只股票，减少重复初始化。
        
        Args:
            strategy: 策略实例
            ts_codes: 股票代码列表
            start_date: 历史数据开始日期
            end_date: 历史数据结束日期
            trade_date: 交易日期
        
        Returns:
            Union[List[str], pd.DataFrame]: 策略筛选结果
        """
        if not ts_codes:
            logger.warning("股票代码列表为空")
            return [] if isinstance(strategy.filter_stocks(pd.DataFrame()), list) else pd.DataFrame()
        
        num_stocks = len(ts_codes)
        
        # 获取策略类和参数（用于在子进程中重建策略实例）
        strategy_class = strategy.__class__
        strategy_params = self._extract_strategy_params(strategy)
        
        # 计算批次大小
        if self.batch_size is None:
            # 自动计算：确保每个进程至少处理一只股票，但尽量让每个进程处理多只股票
            batch_size = max(1, num_stocks // self.max_workers)
        else:
            batch_size = max(1, self.batch_size)
        
        # 将股票列表分批
        batches = []
        for i in range(0, num_stocks, batch_size):
            batch = ts_codes[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"将 {num_stocks} 只股票分为 {len(batches)} 批，每批约 {batch_size} 只股票")
        
        # 使用进程池并行处理
        results = []
        stats = {
            'total': num_stocks,
            'no_data': 0,
            'no_today_data': 0,
            'strategy_filtered': 0,
            'errors': 0
        }
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有批次任务
            future_to_batch = {
                executor.submit(process_batch_stocks_complete, batch, strategy_class, strategy_params, 
                               start_date, end_date, trade_date, self.config): batch
                for batch in batches
            }
            
            # 使用tqdm显示进度条
            with tqdm(total=num_stocks, desc=f"处理股票 ({strategy.name})", unit="只") as pbar:
                # 收集结果
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        batch_results = future.result()
                        # batch_results 是一个列表，包含该批次每只股票的结果
                        for result in batch_results:
                            if result is not None:
                                results.append(result)
                                stats['strategy_filtered'] += 1
                            else:
                                stats['no_data'] += 1
                            pbar.set_postfix({"筛选": len(results)})
                            pbar.update(1)
                    except ValueError as e:
                        # 这是数据缺失的错误
                        if "既没有历史K线数据" in str(e) or "实时K线数据聚合失败" in str(e):
                            stats['no_today_data'] += len(batch)
                        # 更新进度条
                        for _ in batch:
                            pbar.update(1)
                    except Exception as e:
                        stats['errors'] += len(batch)
                        logger.error(f"处理批次时出错: {e}")
                        # 更新进度条
                        for _ in batch:
                            pbar.update(1)
        
        # 输出统计信息
        logger.info("=" * 60)
        logger.info("策略筛选统计信息")
        logger.info("=" * 60)
        logger.info(f"总股票数: {stats['total']}")
        logger.info(f"无历史数据: {stats['no_data']}")
        logger.info(f"缺少今天数据: {stats['no_today_data']}")
        logger.info(f"策略筛选通过: {stats['strategy_filtered']}")
        logger.info(f"处理错误: {stats['errors']}")
        logger.info("=" * 60)
        
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
    
    def _update_real_time_data(self, trade_date: str) -> None:
        """
        更新实时K线数据
        
        从 DailyPipeline 移至此处的功能，用于在运行策略前更新实时K线数据
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)
        """
        try:
            raw_data = self.intraday_kline_collector.collect()
            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到实时数据，日期:{trade_date}")
                return
            
            transformed_data = self.intraday_kline_transformer.transform(raw_data, trade_date=trade_date)
            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的实时数据为空，日期:{trade_date}")
                return

            self.intraday_kline_loader.load(transformed_data, strategy=BaseLoader.LOAD_STRATEGY_APPEND)

        except Exception as e:
            logger.error(f"更新实时数据失败，日期:{trade_date}，错误:{e}")
            raise

