"""
日K线数据转换器

负责清洗、标准化日K线数据
"""

from typing import Any, Dict
import pandas as pd
import numpy as np
from loguru import logger

from core.transformers.base import BaseTransformer
from core.common.exceptions import TransformerException
from utils.date_helper import DateHelper


class DailyKlineTransformer(BaseTransformer):
    """
    日K线数据转换器
    
    对采集到的日K线数据进行清洗、标准化处理：
    - 字段重命名和映射
    - 数据类型转换
    - 剔除停牌数据（如果配置了 remove_halted）
    - 验证 OHLC 关系（如果配置了 validate_ohlc）
    - 日期格式标准化
    """
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        转换日K线数据
        
        Args:
            data: 原始日K线数据
            
        Returns:
            pd.DataFrame: 转换后的日K线数据，包含标准化后的字段
            
        Raises:
            TransformerException: 转换失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("输入数据为空，返回空 DataFrame")
            return pd.DataFrame()
        
        logger.info(f"开始转换日K线数据，数据量: {len(data)}")
        
        try:
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 1. 字段重命名（如果需要）
            # Tushare API 返回的字段名通常已经是标准格式，但为了兼容性，可以添加映射
            column_mapping = self.transform_rules.get("column_mapping", {})
            if column_mapping:
                df = self._rename_columns(df, column_mapping)
            
            # 2. 标准化日期格式
            if 'trade_date' in df.columns:
                df['trade_date'] = df['trade_date'].apply(
                    lambda x: DateHelper.normalize_to_yyyy_mm_dd(str(x)) if pd.notna(x) else None
                )
            
            # 3. 数据类型转换
            numeric_columns = ['open', 'high', 'low', 'close', 'vol', 'amount', 'change', 'pct_chg']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 4. 剔除停牌数据（如果配置了 remove_halted）
            if self.transform_rules.get("remove_halted", False):
                initial_count = len(df)
                # 停牌数据通常表现为：vol=0 或 amount=0 或 close=0
                df = df[
                    (df['vol'].fillna(0) > 0) & 
                    (df['amount'].fillna(0) > 0) & 
                    (df['close'].fillna(0) > 0)
                ]
                removed_count = initial_count - len(df)
                if removed_count > 0:
                    logger.info(f"剔除停牌数据: {removed_count} 条")
            
            # 5. 验证 OHLC 关系（如果配置了 validate_ohlc）
            if self.transform_rules.get("validate_ohlc", False):
                initial_count = len(df)
                # 验证：high >= low, high >= open, high >= close, low <= open, low <= close
                invalid_mask = (
                    (df['high'] < df['low']) |
                    (df['high'] < df['open']) |
                    (df['high'] < df['close']) |
                    (df['low'] > df['open']) |
                    (df['low'] > df['close'])
                )
                invalid_count = invalid_mask.sum()
                if invalid_count > 0:
                    logger.warning(f"发现 {invalid_count} 条 OHLC 关系异常的数据，将被剔除")
                    df = df[~invalid_mask]
                    removed_count = initial_count - len(df)
                    if removed_count > 0:
                        logger.info(f"剔除 OHLC 异常数据: {removed_count} 条")
            
            # 6. 处理缺失值（如果配置了 fill_missing）
            if self.transform_rules.get("fill_missing", False):
                # 对于价格数据，使用前一个交易日的收盘价填充
                price_columns = ['open', 'high', 'low', 'close']
                for col in price_columns:
                    if col in df.columns:
                        df[col] = df.groupby('ts_code')[col].ffill()
            
            # 7. 确保必需字段存在
            required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
            
            # 8. 按股票代码和日期排序
            if 'ts_code' in df.columns and 'trade_date' in df.columns:
                df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
            
            logger.info(f"转换完成，最终数据量: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"转换日K线数据失败: {e}")
            raise TransformerException(f"转换日K线数据失败: {e}") from e

