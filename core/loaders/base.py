"""
加载器基类模块

定义所有数据加载器的抽象基类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import pandas as pd
from loguru import logger

from core.common.exceptions import LoaderException


class BaseLoader(ABC):
    """
    加载器抽象基类
    
    所有数据加载器都应该继承此类并实现 load 方法。
    基类提供加载策略（append/replace/upsert）、批量处理等通用功能框架。
    """
    
    # 加载策略常量
    LOAD_STRATEGY_APPEND = "append"  # 追加数据
    LOAD_STRATEGY_REPLACE = "replace"  # 替换数据
    LOAD_STRATEGY_UPSERT = "upsert"  # 存在则更新，不存在则插入
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化加载器
        
        Args:
            config: 加载器配置字典，包含表名、加载策略、批量大小等配置
        """
        self.config = config or {}
        self.table = self.config.get("table", "")
        self.load_strategy = self.config.get("load_strategy", self.LOAD_STRATEGY_UPSERT)
        self.batch_size = self.config.get("batch_size", 1000)
        self.upsert_keys = self.config.get("upsert_keys", [])
        logger.debug(f"初始化加载器: {self.__class__.__name__}, 表: {self.table}, 策略: {self.load_strategy}")
    
    @abstractmethod
    def load(self, data: pd.DataFrame) -> None:
        """
        加载数据到数据库的核心方法
        
        Args:
            data: 待加载的数据 DataFrame
            
        Raises:
            LoaderException: 当加载失败时抛出异常
        """
        pass
    
    def _load_append(self, data: pd.DataFrame) -> None:
        """
        追加模式加载数据（框架方法）
        
        Args:
            data: 待加载的数据
        """
        raise NotImplementedError("_load_append 方法待实现")
    
    def _load_replace(self, data: pd.DataFrame) -> None:
        """
        替换模式加载数据（框架方法）
        
        Args:
            data: 待加载的数据
        """
        raise NotImplementedError("_load_replace 方法待实现")
    
    def _load_upsert(self, data: pd.DataFrame) -> None:
        """
        更新或插入模式加载数据（框架方法）
        
        Args:
            data: 待加载的数据
        """
        raise NotImplementedError("_load_upsert 方法待实现")
    
    def _batch_load(self, data: pd.DataFrame) -> None:
        """
        批量加载数据（框架方法）
        
        Args:
            data: 待加载的数据
        """
        raise NotImplementedError("_batch_load 方法待实现")
    
    def _validate_data_before_load(self, data: pd.DataFrame) -> bool:
        """
        加载前验证数据（框架方法）
        
        Args:
            data: 待验证的数据
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            LoaderException: 验证失败时抛出异常
        """
        raise NotImplementedError("_validate_data_before_load 方法待实现")

