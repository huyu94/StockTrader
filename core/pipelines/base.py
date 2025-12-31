"""
流水线基类模块

定义所有 ETL Pipeline 的抽象基类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.transformers.base import BaseTransformer
from core.loaders.base import BaseLoader
from core.common.exceptions import PipelineException


class BasePipeline(ABC):
    """
    流水线抽象基类
    
    所有 ETL Pipeline 都应该继承此类并实现 run 方法。
    基类提供 Collector、Transformer、Loader 的组合和 ETL 流程编排框架。
    """
    
    def __init__(
        self,
        collector: BaseCollector,
        transformer: BaseTransformer,
        loader: BaseLoader,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化流水线
        
        Args:
            collector: 数据采集器实例
            transformer: 数据转换器实例
            loader: 数据加载器实例
            config: 流水线配置字典
        """
        self.collector = collector
        self.transformer = transformer
        self.loader = loader
        self.config = config or {}
        logger.debug(f"初始化流水线: {self.__class__.__name__}")
    
    @abstractmethod
    def run(self, **kwargs) -> None:
        """
        执行 ETL 流程的核心方法
        
        典型的执行流程：
        1. Extract - 采集数据
        2. Transform - 转换数据
        3. Load - 加载数据
        
        Args:
            **kwargs: 流水线执行参数，如 stock_codes, start_date, end_date 等
            
        Raises:
            PipelineException: 当执行失败时抛出异常
        """
        pass
    
    def run_incremental(self, stock_codes: List[str], days_back: int = 1) -> None:
        """
        增量更新模式（框架方法）
        
        Args:
            stock_codes: 股票代码列表
            days_back: 回溯天数，默认 1 天（更新最近一天的数据）
        """
        raise NotImplementedError("run_incremental 方法待实现")
    
    def run_full(self, stock_codes: List[str]) -> None:
        """
        全量更新模式（框架方法）
        
        Args:
            stock_codes: 股票代码列表
        """
        raise NotImplementedError("run_full 方法待实现")
    
    def _execute_etl(self, params: Dict[str, Any]) -> None:
        """
        执行 ETL 流程的通用方法（框架方法）
        
        Args:
            params: 采集参数
        """
        logger.info(f"开始执行 ETL 流程: {self.__class__.__name__}")
        # 1. Extract - 采集数据
        # raw_data = self.collector.collect(params)
        # 2. Transform - 转换数据
        # clean_data = self.transformer.transform(raw_data)
        # 3. Load - 加载数据
        # self.loader.load(clean_data)
        raise NotImplementedError("_execute_etl 方法待实现")

