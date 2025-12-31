"""
股票基本信息加载器

负责将处理后的股票基本信息数据持久化到数据库
"""

from typing import Any, Dict, List
import pandas as pd
from loguru import logger
from datetime import datetime

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException
from utils.date_helper import DateHelper
from core.models.orm import BasicInfoORM


class BasicInfoLoader(BaseLoader):
    """
    股票基本信息加载器
    
    将转换后的股票基本信息数据加载到数据库表中
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化股票基本信息加载器
        
        Args:
            config: 配置字典，包含：
                - table: 表名（默认 "basic_info"）
                - load_strategy: 加载策略（append/replace/upsert，默认 upsert）
                - batch_size: 批量大小（默认 1000）
                - upsert_keys: upsert 的键（默认 ['ts_code']）
        """
        if config is None:
            config = {}
        if 'table' not in config:
            config['table'] = 'basic_info'
        if 'upsert_keys' not in config:
            config['upsert_keys'] = ['ts_code']
        
        super().__init__(config)
    
    def _get_orm_model(self):
        """获取对应的ORM模型类"""
        if BasicInfoORM is None:
            raise LoaderException("BasicInfoORM 未导入，请检查依赖")
        return BasicInfoORM
    
    def _get_required_columns(self) -> List[str]:
        """获取必需的数据列"""
        return ['ts_code', 'symbol', 'name']
    
    def load(self, data: pd.DataFrame) -> None:
        """
        加载股票基本信息数据到数据库
        
        Args:
            data: 待加载的股票基本信息数据 DataFrame
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("股票基本信息数据为空，跳过加载")
            return
        
        logger.info(f"开始加载股票基本信息到表 {self.table}，数据量: {len(data)}")
        
        try:
            # 添加更新时间（如果配置需要）
            data_copy = data.copy()
            if 'updated_at' not in data_copy.columns:
                today = DateHelper.today()
                data_copy['updated_at'] = today
            
            # 根据加载策略选择加载方式
            if self.load_strategy == self.LOAD_STRATEGY_APPEND:
                self._load_append(data_copy)
            elif self.load_strategy == self.LOAD_STRATEGY_REPLACE:
                self._load_replace(data_copy)
            elif self.load_strategy == self.LOAD_STRATEGY_UPSERT:
                self._load_upsert(data_copy)
            else:
                raise LoaderException(f"不支持的加载策略: {self.load_strategy}")
            
            logger.info(f"股票基本信息数据加载完成，表: {self.table}")
            
        except Exception as e:
            logger.error(f"加载股票基本信息数据失败: {e}")
            raise LoaderException(f"加载股票基本信息数据失败: {e}") from e

