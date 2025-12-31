"""
复权因子转换器

负责清洗、标准化复权因子数据
"""

from typing import Any, Dict
import pandas as pd
from loguru import logger

from core.transformers.base import BaseTransformer
from core.common.exceptions import TransformerException
from utils.date_helper import DateHelper


class AdjFactorTransformer(BaseTransformer):
    """
    复权因子转换器
    
    对采集到的复权因子数据进行清洗、标准化处理：
    - 字段重命名和映射
    - 数据类型转换
    - 日期格式标准化
    - 过滤无效数据（adj_factor <= 0）
    """
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        转换复权因子数据
        
        Args:
            data: 原始复权因子数据
            
        Returns:
            pd.DataFrame: 转换后的复权因子数据
            
        Raises:
            TransformerException: 转换失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("输入数据为空，返回空 DataFrame")
            return pd.DataFrame()
        
        logger.info(f"开始转换复权因子数据，数据量: {len(data)}")
        
        try:
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 1. 字段重命名（如果需要）
            column_mapping = self.transform_rules.get("column_mapping", {})
            if column_mapping:
                df = self._rename_columns(df, column_mapping)
            
            # 2. 标准化日期格式
            if 'trade_date' in df.columns:
                df['trade_date'] = df['trade_date'].apply(
                    lambda x: DateHelper.normalize_to_yyyy_mm_dd(str(x)) if pd.notna(x) else None
                )
            
            if 'update_time' in df.columns:
                df['update_time'] = df['update_time'].apply(
                    lambda x: DateHelper.normalize_to_yyyy_mm_dd(str(x)) if pd.notna(x) else None
                )
            
            # 3. 数据类型转换
            if 'adj_factor' in df.columns:
                df['adj_factor'] = pd.to_numeric(df['adj_factor'], errors='coerce')
            
            # 4. 过滤无效数据（adj_factor <= 0 或为空）
            initial_count = len(df)
            if 'adj_factor' in df.columns:
                df = df[(df['adj_factor'].notna()) & (df['adj_factor'] > 0)]
                removed_count = initial_count - len(df)
                if removed_count > 0:
                    logger.info(f"过滤无效复权因子数据: {removed_count} 条")
            
            # 5. 确保必需字段存在
            required_columns = ['ts_code', 'trade_date', 'adj_factor']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
            
            # 6. 按股票代码和日期排序
            if 'ts_code' in df.columns and 'trade_date' in df.columns:
                df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
            
            logger.info(f"转换完成，最终数据量: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"转换复权因子数据失败: {e}")
            raise TransformerException(f"转换复权因子数据失败: {e}") from e

