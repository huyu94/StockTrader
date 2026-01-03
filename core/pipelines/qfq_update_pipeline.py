"""
前复权更新流水线

负责更新股票的前复权价格信息
流程：从数据库读取日K线数据 -> 从数据库读取复权因子数据 -> 计算前复权价格 -> 更新到数据库
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from loguru import logger

from core.pipelines.base import BasePipeline
from core.loaders.base import BaseLoader
from core.common.exceptions import PipelineException
from core.loaders.daily_kline import DailyKlineLoader
from core.loaders.adj_factor import AdjFactorLoader
from core.calculators.qfq_calculator import QFQCalculator
from utils.date_helper import DateHelper


class QFQUpdatePipeline(BasePipeline):
    """
    前复权更新流水线
    
    用于更新股票的前复权价格信息
    # TODO: 更新数据库中所有股票的，如果有股票没有复权因子，或者股票有一段时间没有复权因子，就按复权因子=1计算前复权数据


    流程：
    1. 从数据库读取日K线数据（未复权价格）
    2. 从数据库读取复权因子数据
    3. 计算前复权价格
    4. 更新到数据库（只更新前复权价格字段）
    """
    
    def __init__(
        self,
        kline_loader: Optional[BaseLoader] = None,
        adj_factor_loader: Optional[BaseLoader] = None,
        loader: Optional[BaseLoader] = None,
        qfq_calculator: Optional[QFQCalculator] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化前复权更新流水线
        
        Args:
            kline_loader: 日K线数据加载器（用于读取和写入，如果为 None，则使用默认的 DailyKlineLoader）
            adj_factor_loader: 复权因子加载器（用于读取，如果为 None，则使用默认的 AdjFactorLoader）
            loader: 数据加载器（用于写入，如果为 None，则使用 kline_loader）
            qfq_calculator: 前复权计算器（如果为 None，则创建新的实例）
            config: 流水线配置字典
        """
        # 如果未提供加载器，使用默认的
        if kline_loader is None:
            kline_loader_config = config.get("kline_loader", {}) if config else {}
            if 'load_strategy' not in kline_loader_config:
                kline_loader_config['load_strategy'] = 'upsert'  # 默认使用 upsert 策略
            kline_loader = DailyKlineLoader(config=kline_loader_config)
        
        if adj_factor_loader is None:
            adj_factor_loader_config = config.get("adj_factor_loader", {}) if config else {}
            adj_factor_loader = AdjFactorLoader(config=adj_factor_loader_config)
        
        # 如果未提供写入用的 loader，使用 kline_loader
        # 配置为只更新前复权价格模式
        if loader is None:
            loader_config = config.get("loader", {}) if config else {}
            if 'load_strategy' not in loader_config:
                loader_config['load_strategy'] = 'upsert'  # 默认使用 upsert 策略
            loader_config['update_qfq_only'] = True  # 标记为只更新前复权价格
            loader = DailyKlineLoader(config=loader_config)
        else:
            # 如果提供了 loader，也需要设置 update_qfq_only 标志
            if hasattr(loader, 'config'):
                loader.config['update_qfq_only'] = True
        
        # 保存组件
        self.kline_loader = kline_loader
        self.adj_factor_loader = adj_factor_loader
        self.qfq_calculator = qfq_calculator or QFQCalculator()
        
        # BasePipeline 需要 collector, transformer, loader，我们使用 None 和 loader 作为占位符
        # 因为这里不需要 collector 和 transformer
        from core.collectors.base import BaseCollector
        from core.transformers.base import BaseTransformer
        
        # 创建占位符（不会被使用）
        class DummyCollector(BaseCollector):
            def collect(self, params):
                raise NotImplementedError("QFQUpdatePipeline 不使用 Collector")
        
        class DummyTransformer(BaseTransformer):
            def transform(self, data):
                raise NotImplementedError("QFQUpdatePipeline 不使用 Transformer")
        
        super().__init__(DummyCollector(), DummyTransformer(), loader, config)
    
    def run(
        self,
        ts_codes: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        执行前复权更新流水线
        
        Args:
            ts_codes: 股票代码，可以是单个字符串或字符串列表（可选，如果不提供则更新所有股票）
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            **kwargs: 其他参数
        """
        try:
            logger.info("=" * 60)
            logger.info("开始执行前复权更新流水线")
            logger.info("=" * 60)
            
            # 1. 从数据库读取日K线数据（未复权价格）
            logger.info("步骤 1: 从数据库读取日K线数据（未复权价格）...")
            kline_data = self.kline_loader.read(ts_codes=ts_codes, start_date=start_date, end_date=end_date)
            
            if kline_data is None or kline_data.empty:
                logger.warning("数据库中未找到日K线数据，无法计算前复权价格")
                return
            
            logger.info(f"✓ 读取完成，日K线数据量: {len(kline_data)} 条")
            
            # 2. 从数据库读取复权因子数据（读取所有历史除权除息日的数据）
            logger.info("步骤 2: 从数据库读取复权因子数据（所有历史除权除息日）...")
            adj_factor_data = self.adj_factor_loader.read(ts_codes=ts_codes)
            
            if adj_factor_data is None or adj_factor_data.empty:
                logger.warning("数据库中未找到复权因子数据，无法计算前复权价格")
                return
            
            logger.info(f"✓ 读取完成，复权因子数据量: {len(adj_factor_data)} 条")
            
            # 3. 计算前复权价格
            logger.info("步骤 3: 计算前复权价格...")
            result_data = self.qfq_calculator.calculate(kline_data, adj_factor_data)
            
            if result_data is None or result_data.empty:
                logger.warning("计算前复权价格后数据为空")
                return
            
            # 检查是否包含前复权价格列
            qfq_columns = ['close_qfq', 'open_qfq', 'high_qfq', 'low_qfq']
            has_qfq_data = any(col in result_data.columns for col in qfq_columns)
            
            if not has_qfq_data:
                logger.warning("计算结果中未包含前复权价格列")
                return
            
            # 统计成功计算前复权价格的记录数
            qfq_count = 0
            for col in qfq_columns:
                if col in result_data.columns:
                    qfq_count = result_data[col].notna().sum()
                    break
            
            logger.info(f"✓ 计算完成，成功计算前复权价格的记录数: {qfq_count} 条")
            
            # 4. 更新到数据库（只更新前复权价格字段）
            logger.info("步骤 4: 更新前复权价格到数据库...")
            
            # 只选择需要更新的列（前复权价格列 + 主键列）
            update_columns = ['ts_code', 'trade_date'] + qfq_columns
            available_columns = [col for col in update_columns if col in result_data.columns]
            
            if len(available_columns) < 3:  # 至少需要 ts_code, trade_date 和一个前复权价格列
                logger.warning("没有足够的前复权价格数据可更新")
                return
            
            update_data = result_data[available_columns].copy()
            
            # 过滤掉前复权价格为空的记录
            qfq_data_columns = [col for col in qfq_columns if col in update_data.columns]
            if qfq_data_columns:
                initial_count = len(update_data)
                update_data = update_data.dropna(subset=qfq_data_columns)
                filtered_count = len(update_data)
                if initial_count > filtered_count:
                    logger.info(f"过滤掉 {initial_count - filtered_count} 条前复权价格为空的记录")
            
            if update_data.empty:
                logger.warning("没有有效的前复权价格数据可更新")
                return
            
            # 使用 loader 更新数据（upsert 策略，只更新前复权价格字段）
            self.loader.load(update_data)
            
            logger.info("=" * 60)
            logger.info(f"前复权更新流水线执行完成！")
            logger.info(f"共更新 {len(update_data)} 条记录的前复权价格")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"执行前复权更新流水线失败: {e}")
            raise PipelineException(f"执行前复权更新流水线失败: {e}") from e
    
    def run_for_stock(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> None:
        """
        为单只股票更新前复权价格
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
        """
        self.run(ts_codes=[ts_code], start_date=start_date, end_date=end_date)
    
    def run_for_stocks(
        self,
        ts_codes: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> None:
        """
        为多只股票更新前复权价格
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
        """
        self.run(ts_codes=ts_codes, start_date=start_date, end_date=end_date)
    
    def run_for_date_range(
        self,
        start_date: str,
        end_date: str,
        ts_codes: Optional[List[str]] = None
    ) -> None:
        """
        为指定日期范围更新前复权价格
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)
            ts_codes: 股票代码列表（可选，如果不提供则更新所有股票）
        """
        self.run(ts_codes=ts_codes, start_date=start_date, end_date=end_date)

