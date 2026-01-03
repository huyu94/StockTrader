"""
分时K线数据加载器

负责将处理后的分时K线数据持久化到数据库
"""

from typing import Any, Dict, List
import pandas as pd
from loguru import logger

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException
from core.models.orm import IntradayKlineORM


class IntradayKlineLoader(BaseLoader):
    """
    分时K线数据加载器
    
    将转换后的分时K线数据加载到数据库表中
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化分时K线数据加载器
        
        Args:
            config: 配置字典，包含：
                - table: 表名（默认 "intraday_kline"）
                - batch_size: 批量大小（默认 1000）
                - upsert_keys: upsert 的键（默认 ['ts_code', 'trade_date', 'time']）
        """
        if config is None:
            config = {}
        if 'table' not in config:
            config['table'] = 'intraday_kline'
        if 'upsert_keys' not in config:
            config['upsert_keys'] = ['ts_code', 'trade_date', 'time']
        
        super().__init__(config)
    
    def _get_orm_model(self):
        """获取对应的ORM模型类"""
        if IntradayKlineORM is None:
            raise LoaderException("IntradayKlineORM 未导入，请检查依赖")
        return IntradayKlineORM
    
    def _get_required_columns(self) -> List[str]:
        """获取必需的数据列"""
        return ['ts_code', 'trade_date', 'time', 'price', 'volume', 'amount']
    
    def load(self, data: pd.DataFrame, strategy: str) -> None:
        """
        加载分时K线数据到数据库
        
        Args:
            data: 待加载的分时K线数据 DataFrame
            strategy: 加载策略（append/replace/upsert）
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("分时K线数据为空，跳过加载")
            return
        
        logger.info(f"开始加载分时K线数据到表 {self.table}，数据量: {len(data)}")
        
        try:
            # 根据加载策略选择加载方式
            if strategy == self.LOAD_STRATEGY_APPEND:
                self._load_append(data)
            elif strategy == self.LOAD_STRATEGY_REPLACE:
                self._load_replace(data)
            elif strategy == self.LOAD_STRATEGY_UPSERT:
                self._load_upsert(data)
            else:
                raise LoaderException(f"不支持的加载策略: {strategy}")
            
            logger.info(f"分时K线数据加载完成，表: {self.table}")
            
        except Exception as e:
            logger.error(f"加载分时K线数据失败: {e}")
            raise LoaderException(f"加载分时K线数据失败: {e}") from e

