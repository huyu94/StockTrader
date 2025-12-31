"""
交易日历加载器

负责将处理后的交易日历数据持久化到数据库
"""

from typing import Any, Dict, List
import pandas as pd
from loguru import logger

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException
from core.models.orm import TradeCalendarORM


class TradeCalendarLoader(BaseLoader):
    """
    交易日历加载器
    
    将转换后的交易日历数据加载到数据库表中
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化交易日历加载器
        
        Args:
            config: 配置字典，包含：
                - table: 表名（默认 "trade_calendar"）
                - load_strategy: 加载策略（append/replace/upsert，默认 upsert）
                - batch_size: 批量大小（默认 1000）
                - upsert_keys: upsert 的键（默认 ['cal_date']）
                - preserve_null_columns: 保留NULL的列（默认所有 _open 列）
        """
        if config is None:
            config = {}
        if 'table' not in config:
            config['table'] = 'trade_calendar'
        if 'upsert_keys' not in config:
            config['upsert_keys'] = ['cal_date']
        if 'preserve_null_columns' not in config:
            # 默认保留所有 _open 列的现有值（如果新值为NULL）
            config['preserve_null_columns'] = [
                'sse_open', 'szse_open', 'cffex_open', 
                'shfe_open', 'czce_open', 'dce_open', 'ine_open'
            ]
        
        super().__init__(config)
    
    def _get_orm_model(self):
        """获取对应的ORM模型类"""
        if TradeCalendarORM is None:
            raise LoaderException("TradeCalendarORM 未导入，请检查依赖")
        return TradeCalendarORM
    
    def _get_required_columns(self) -> List[str]:
        """获取必需的数据列"""
        return ['cal_date']
    
    def load(self, data: pd.DataFrame) -> None:
        """
        加载交易日历数据到数据库
        
        Args:
            data: 待加载的交易日历数据 DataFrame
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("交易日历数据为空，跳过加载")
            return
        
        logger.info(f"开始加载交易日历到表 {self.table}，数据量: {len(data)}")
        
        try:
            # 根据加载策略选择加载方式
            # 交易日历通常使用 upsert 策略，并且需要保留现有交易所的值
            if self.load_strategy == self.LOAD_STRATEGY_APPEND:
                self._load_append(data)
            elif self.load_strategy == self.LOAD_STRATEGY_REPLACE:
                self._load_replace(data)
            elif self.load_strategy == self.LOAD_STRATEGY_UPSERT:
                self._load_upsert(data)
            else:
                raise LoaderException(f"不支持的加载策略: {self.load_strategy}")
            
            logger.info(f"交易日历数据加载完成，表: {self.table}")
            
        except Exception as e:
            logger.error(f"加载交易日历数据失败: {e}")
            raise LoaderException(f"加载交易日历数据失败: {e}") from e

