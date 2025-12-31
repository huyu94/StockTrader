"""
复权因子加载器

负责将处理后的复权因子数据持久化到数据库
"""

from typing import Any, Dict
import pandas as pd
from loguru import logger

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException


class AdjFactorLoader(BaseLoader):
    """
    复权因子加载器
    
    将转换后的复权因子数据加载到数据库表中
    """
    
    def load(self, data: pd.DataFrame) -> None:
        """
        加载复权因子数据到数据库
        
        Args:
            data: 待加载的复权因子数据 DataFrame
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        logger.info(f"开始加载复权因子数据到表 {self.table}，数据量: {len(data)}")
        raise NotImplementedError("AdjFactorLoader.load 方法待实现")

