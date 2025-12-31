"""
股票基本信息转换器

负责清洗、标准化股票基本信息数据
"""

from typing import Any, Dict
import pandas as pd
from loguru import logger

from core.transformers.base import BaseTransformer
from core.common.exceptions import TransformerException
from utils.date_helper import DateHelper


class BasicInfoTransformer(BaseTransformer):
    """
    股票基本信息转换器
    
    对采集到的股票基本信息数据进行清洗、标准化处理：
    - 字段重命名和映射
    - 数据类型转换
    - 日期格式标准化
    - 数据去重
    """
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        转换股票基本信息数据
        
        Args:
            data: 原始股票基本信息数据
            
        Returns:
            pd.DataFrame: 转换后的股票基本信息数据
            
        Raises:
            TransformerException: 转换失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("输入数据为空，返回空 DataFrame")
            return pd.DataFrame()
        
        logger.info(f"开始转换股票基本信息，数据量: {len(data)}")
        
        try:
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 1. 字段重命名（如果需要）
            column_mapping = self.transform_rules.get("column_mapping", {})
            if column_mapping:
                df = self._rename_columns(df, column_mapping)
            
            # 2. 标准化日期格式
            if 'list_date' in df.columns:
                df['list_date'] = df['list_date'].apply(
                    lambda x: DateHelper.normalize_to_yyyy_mm_dd(str(x)) if pd.notna(x) else None
                )
            
            # 3. 确保 symbol 字段存在（如果没有，从 ts_code 提取）
            if 'symbol' not in df.columns and 'ts_code' in df.columns:
                df['symbol'] = df['ts_code'].str.split('.').str[0]
            
            # 4. 确保 name 字段存在（如果没有，使用 ts_code 作为默认值）
            if 'name' not in df.columns:
                if 'ts_code' in df.columns:
                    df['name'] = df['ts_code']
                else:
                    df['name'] = ''
            
            # 5. 数据去重（基于 ts_code）
            initial_count = len(df)
            if 'ts_code' in df.columns:
                df = df.drop_duplicates(subset=['ts_code'], keep='last')
                removed_count = initial_count - len(df)
                if removed_count > 0:
                    logger.info(f"去除重复数据: {removed_count} 条")
            
            # 6. 确保必需字段存在
            required_columns = ['ts_code', 'symbol', 'name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
            
            # 7. 按股票代码排序
            if 'ts_code' in df.columns:
                df = df.sort_values('ts_code').reset_index(drop=True)
            
            logger.info(f"转换完成，最终数据量: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"转换股票基本信息失败: {e}")
            raise TransformerException(f"转换股票基本信息失败: {e}") from e

