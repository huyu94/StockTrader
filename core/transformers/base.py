"""
转换器基类模块

定义所有数据转换器的抽象基类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from loguru import logger

from core.common.exceptions import TransformerException


class BaseTransformer(ABC):
    """
    转换器抽象基类
    
    所有数据转换器都应该继承此类并实现 transform 方法。
    基类提供转换规则配置、数据验证等通用功能框架。
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化转换器
        
        Args:
            config: 转换器配置字典，包含转换规则、验证规则等配置
        """
        self.config = config or {}
        self.transform_rules = self.config.get("transform_rules", {})
        logger.debug(f"初始化转换器: {self.__class__.__name__}")
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        转换数据的核心方法
        
        Args:
            data: 原始数据 DataFrame
            
        Returns:
            pd.DataFrame: 转换后的数据 DataFrame
            
        Raises:
            TransformerException: 当转换失败时抛出异常
        """
        pass
    
    def _rename_columns(self, data: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        重命名列（框架方法）
        
        Args:
            data: 待转换的数据
            column_mapping: 列名映射字典 {旧列名: 新列名}
            
        Returns:
            pd.DataFrame: 重命名后的数据
        """
        raise NotImplementedError("_rename_columns 方法待实现")
    
    def _convert_types(self, data: pd.DataFrame, type_mapping: Dict[str, type]) -> pd.DataFrame:
        """
        转换数据类型（框架方法）
        
        Args:
            data: 待转换的数据
            type_mapping: 类型映射字典 {列名: 目标类型}
            
        Returns:
            pd.DataFrame: 类型转换后的数据
        """
        raise NotImplementedError("_convert_types 方法待实现")
    
    def _handle_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        处理缺失值（框架方法）
        
        Args:
            data: 待处理的数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        raise NotImplementedError("_handle_missing_values 方法待实现")
    
    def _handle_outliers(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        处理异常值（框架方法）
        
        Args:
            data: 待处理的数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        raise NotImplementedError("_handle_outliers 方法待实现")
    
    def _validate_data(self, data: pd.DataFrame) -> bool:
        """
        验证数据质量（框架方法）
        
        Args:
            data: 待验证的数据
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            TransformerException: 验证失败时抛出异常
        """
        raise NotImplementedError("_validate_data 方法待实现")

