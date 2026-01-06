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
        重命名列
        
        Args:
            data: 待转换的数据
            column_mapping: 列名映射字典 {旧列名: 新列名}
            
        Returns:
            pd.DataFrame: 重命名后的数据
        """
        if data is None or data.empty:
            return data
        
        # 只重命名存在的列
        existing_mapping = {old: new for old, new in column_mapping.items() if old in data.columns}
        
        if existing_mapping:
            data = data.rename(columns=existing_mapping)
            logger.debug(f"重命名列: {existing_mapping}")
        
        return data
    
    def _convert_types(self, data: pd.DataFrame, type_mapping: Dict[str, type]) -> pd.DataFrame:
        """
        转换数据类型
        
        Args:
            data: 待转换的数据
            type_mapping: 类型映射字典 {列名: 目标类型}
            
        Returns:
            pd.DataFrame: 类型转换后的数据
        """
        if data is None or data.empty:
            return data
        
        data = data.copy()
        
        for column, target_type in type_mapping.items():
            if column in data.columns:
                try:
                    if target_type == str:
                        data[column] = data[column].astype(str)
                    elif target_type == int:
                        data[column] = pd.to_numeric(data[column], errors='coerce').astype('Int64')
                    elif target_type == float:
                        data[column] = pd.to_numeric(data[column], errors='coerce')
                    else:
                        data[column] = data[column].astype(target_type)
                    logger.debug(f"转换列 {column} 类型为 {target_type}")
                except Exception as e:
                    logger.warning(f"转换列 {column} 类型失败: {e}")
        
        return data
    
    def _handle_missing_values(self, data: pd.DataFrame, strategy: str = "drop", fill_value: Any = None) -> pd.DataFrame:
        """
        处理缺失值
        
        Args:
            data: 待处理的数据
            strategy: 处理策略，可选值：
                - "drop": 删除包含缺失值的行
                - "fill": 填充缺失值（需要提供 fill_value）
                - "forward": 前向填充
                - "backward": 后向填充
            fill_value: 填充值（当 strategy="fill" 时使用）
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        if data is None or data.empty:
            return data
        
        data = data.copy()
        initial_count = len(data)
        
        if strategy == "drop":
            data = data.dropna()
        elif strategy == "fill":
            if fill_value is not None:
                data = data.fillna(fill_value)
        elif strategy == "forward":
            data = data.ffill()
        elif strategy == "backward":
            data = data.bfill()
        
        if len(data) < initial_count:
            logger.debug(f"处理缺失值: 从 {initial_count} 条减少到 {len(data)} 条")
        
        return data
    
    def _handle_outliers(self, data: pd.DataFrame, method: str = "clip", columns: list = None) -> pd.DataFrame:
        """
        处理异常值
        
        Args:
            data: 待处理的数据
            method: 处理方法，可选值：
                - "clip": 裁剪到上下限
                - "remove": 删除异常值
            columns: 要处理的列名列表，如果为 None 则处理所有数值列
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        if data is None or data.empty:
            return data
        
        data = data.copy()
        
        if columns is None:
            # 自动选择数值列
            columns = data.select_dtypes(include=['int64', 'float64']).columns.tolist()
        
        for column in columns:
            if column not in data.columns:
                continue
            
            # 使用 IQR 方法检测异常值
            Q1 = data[column].quantile(0.25)
            Q3 = data[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            if method == "clip":
                data[column] = data[column].clip(lower=lower_bound, upper=upper_bound)
                logger.debug(f"裁剪列 {column} 的异常值: [{lower_bound}, {upper_bound}]")
            elif method == "remove":
                initial_count = len(data)
                data = data[(data[column] >= lower_bound) & (data[column] <= upper_bound)]
                if len(data) < initial_count:
                    logger.debug(f"删除列 {column} 的异常值: 从 {initial_count} 条减少到 {len(data)} 条")
        
        return data
    
    def _validate_data(self, data: pd.DataFrame, required_columns: list = None, validation_rules: Dict[str, Any] = None) -> bool:
        """
        验证数据质量
        
        Args:
            data: 待验证的数据
            required_columns: 必需的列名列表
            validation_rules: 验证规则字典，格式如：
                {
                    'column_name': {
                        'min': 最小值,
                        'max': 最大值,
                        'not_null': True/False,
                        'unique': True/False
                    }
                }
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            TransformerException: 验证失败时抛出异常
        """
        if data is None or data.empty:
            raise TransformerException("数据为空，无法验证")
        
        # 验证必需列
        if required_columns:
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
        
        # 验证规则
        if validation_rules:
            for column, rules in validation_rules.items():
                if column not in data.columns:
                    continue
                
                if rules.get('not_null', False):
                    null_count = data[column].isna().sum()
                    if null_count > 0:
                        raise TransformerException(f"列 {column} 包含 {null_count} 个空值")
                
                if 'min' in rules:
                    min_value = rules['min']
                    invalid_count = (data[column] < min_value).sum()
                    if invalid_count > 0:
                        raise TransformerException(f"列 {column} 有 {invalid_count} 个值小于最小值 {min_value}")
                
                if 'max' in rules:
                    max_value = rules['max']
                    invalid_count = (data[column] > max_value).sum()
                    if invalid_count > 0:
                        raise TransformerException(f"列 {column} 有 {invalid_count} 个值大于最大值 {max_value}")
                
                if rules.get('unique', False):
                    duplicate_count = data[column].duplicated().sum()
                    if duplicate_count > 0:
                        raise TransformerException(f"列 {column} 有 {duplicate_count} 个重复值")
        
        return True

