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
# 导入各个数据源的组件
from core.collectors.basic_info import BasicInfoCollector
from core.collectors.trade_calendar import TradeCalendarCollector
from core.collectors.daily_kline import DailyKlineCollector
from core.collectors.adj_factor import AdjFactorCollector
from core.collectors.ex_date import ExDateCollector

from core.transformers.basic_info import BasicInfoTransformer
from core.transformers.trade_calendar import TradeCalendarTransformer
from core.transformers.daily_kline import DailyKlineTransformer
from core.transformers.adj_factor import AdjFactorTransformer

from core.loaders.basic_info import BasicInfoLoader
from core.loaders.trade_calendar import TradeCalendarLoader
from core.loaders.daily_kline import DailyKlineLoader
from core.loaders.adj_factor import AdjFactorLoader


class BasePipeline(ABC):
    """
    流水线抽象基类
    
    所有 ETL Pipeline 都应该继承此类并实现 run 方法。
    基类提供 Collector、Transformer、Loader 的组合和 ETL 流程编排框架。
    """
    
    def __init__(
        self,
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
        self.config = config or {}
        logger.debug(f"初始化流水线: {self.__class__.__name__}")


                # 保存各个组件
        self.basic_info_collector = BasicInfoCollector()
        self.basic_info_transformer = BasicInfoTransformer()
        self.basic_info_loader = BasicInfoLoader()
        
        self.trade_calendar_collector = TradeCalendarCollector()
        self.trade_calendar_transformer = TradeCalendarTransformer()
        self.trade_calendar_loader = TradeCalendarLoader()
        
        self.daily_kline_collector = DailyKlineCollector()
        self.daily_kline_transformer = DailyKlineTransformer()
        self.daily_kline_loader = DailyKlineLoader()
        
        self.adj_factor_collector = AdjFactorCollector()
        self.adj_factor_transformer = AdjFactorTransformer()
        self.adj_factor_loader = AdjFactorLoader()

        self.ex_date_collector = ExDateCollector()
