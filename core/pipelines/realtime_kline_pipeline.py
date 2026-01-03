"""
实时K线流水线

负责获取市场所有股票的实时K线数据（使用 akshare 实时行情接口）
只获取和打印数据，不进行入库操作
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger

from core.pipelines.base import BasePipeline
from core.collectors.base import BaseCollector
from core.transformers.base import BaseTransformer
from core.loaders.base import BaseLoader
from core.common.exceptions import PipelineException
from core.collectors.intraday_kline import IntradayKlineCollector
from core.transformers.intraday_kline import IntradayKlineTransformer


class RealtimeKlinePipeline(BasePipeline):
    """
    实时K线流水线
    
    用于获取市场所有股票的实时K线数据（使用 akshare 实时行情接口）
    只获取和打印数据，不进行入库操作
    """
    
    def __init__(
        self,
        collector: Optional[BaseCollector] = None,
        transformer: Optional[BaseTransformer] = None,
        loader: Optional[BaseLoader] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化实时K线流水线
        
        Args:
            collector: 数据采集器实例（如果为 None，则使用默认的 IntradayKlineCollector）
            transformer: 数据转换器实例（如果为 None，则使用默认的 IntradayKlineTransformer）
            loader: 数据加载器实例（可选，因为不进行入库操作）
            config: 流水线配置字典
        """
        # 如果未提供 collector，使用默认的 IntradayKlineCollector
        if collector is None:
            collector_config = config.get("collector", {}) if config else {}
            if "source" not in collector_config:
                collector_config["source"] = "akshare"
            collector = IntradayKlineCollector(config=collector_config)
        
        # 如果未提供 transformer，使用默认的 IntradayKlineTransformer
        if transformer is None:
            transformer_config = config.get("transformer", {}) if config else {}
            transformer = IntradayKlineTransformer(config=transformer_config)
        
        # loader 可以为 None，因为不进行入库操作
        if loader is None:
            # 创建一个空的 loader（BasePipeline 需要，但不会使用）
            from core.loaders.intraday_kline import IntradayKlineLoader
            loader_config = config.get("loader", {}) if config else {}
            loader = IntradayKlineLoader(config=loader_config)
        
        super().__init__(collector, transformer, loader, config)
    
    def run(self, ts_codes: Optional[List[str]] = None, **kwargs) -> None:
        """
        执行实时K线流水线
        
        流程：
        1. 使用 Collector 获取原始实时行情数据（akshare 格式）
        2. 使用 Transformer 转换数据为标准格式
        3. 打印转换后的数据（不进行入库）
        
        Args:
            ts_codes: 股票代码列表（可选，如果不提供则获取全市场数据）
            **kwargs: 其他参数
                - trade_date: 交易日期 (YYYY-MM-DD)（可选，如果不提供则使用当前日期）
        """
        try:
            logger.info("=" * 60)
            logger.info("开始执行实时K线流水线")
            logger.info("=" * 60)
            
            # 1. Extract - 采集原始数据
            logger.info("步骤 1: 采集原始实时行情数据...")
            raw_data = self.collector.collect()
            
            if raw_data is None or raw_data.empty:
                logger.warning("未采集到任何数据")
                return
            
            logger.info(f"✓ 采集完成，原始数据量: {len(raw_data)} 条")
            logger.info(f"原始数据列名: {raw_data.columns.tolist()}")
            
            # 2. Transform - 转换数据
            logger.info("\n步骤 2: 转换数据为标准格式...")
            transform_kwargs = {}
            if ts_codes is not None:
                transform_kwargs["ts_codes"] = ts_codes
            if "trade_date" in kwargs:
                transform_kwargs["trade_date"] = kwargs["trade_date"]
            
            clean_data = self.transformer.transform(raw_data, **transform_kwargs)
            
            if clean_data is None or clean_data.empty:
                logger.warning("转换后数据为空")
                return
            
            logger.info(f"✓ 转换完成，数据量: {len(clean_data)} 条")
            
            # 3. 打印数据（不进行入库）
            logger.info("\n步骤 3: 打印转换后的数据...")
            logger.info("=" * 60)
            logger.info("实时K线数据（转换后）")
            logger.info("=" * 60)
            logger.info(f"数据形状: {clean_data.shape}")
            logger.info(f"列名: {clean_data.columns.tolist()}")
            
            # 打印前20条数据
            if len(clean_data) > 0:
                logger.info(f"\n前20条数据:\n{clean_data.head(20).to_string()}")
                if len(clean_data) > 20:
                    logger.info(f"\n... (共 {len(clean_data)} 条数据，仅显示前20条)")
            
            # 打印数据统计信息
            if len(clean_data) > 0:
                numeric_cols = clean_data.select_dtypes(include=['int64', 'float64']).columns
                if len(numeric_cols) > 0:
                    logger.info(f"\n数据统计:\n{clean_data[numeric_cols].describe().to_string()}")
            
            logger.info("=" * 60)
            logger.info("实时K线流水线执行完成！")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"执行实时K线流水线失败: {e}")
            raise PipelineException(f"执行实时K线流水线失败: {e}") from e







