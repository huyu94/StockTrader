"""
分时K线数据转换器

负责清洗、标准化分时K线数据
"""

from typing import Any, Dict
import pandas as pd
import numpy as np
from loguru import logger

from core.transformers.base import BaseTransformer
from core.common.exceptions import TransformerException
from utils.date_helper import DateHelper


class IntradayKlineTransformer(BaseTransformer):
    """
    分时K线数据转换器
    
    对采集到的分时K线数据进行清洗、标准化处理：
    - 字段重命名和映射
    - 数据类型转换
    - 剔除异常数据
    - 日期时间格式标准化
    """
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        转换分时K线数据
        
        Args:
            data: 原始分时K线数据
            
        Returns:
            pd.DataFrame: 转换后的分时K线数据，包含标准化后的字段
            
        Raises:
            TransformerException: 转换失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("输入数据为空，返回空 DataFrame")
            return pd.DataFrame()
        
        logger.info(f"开始转换分时K线数据，数据量: {len(data)}")
        
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
            
            # 3. 标准化时间格式
            if 'time' in df.columns:
                df['time'] = df['time'].apply(
                    lambda x: self._normalize_time(str(x)) if pd.notna(x) else None
                )
            
            # 4. 构建 datetime 列（如果不存在）
            if 'datetime' not in df.columns:
                if 'trade_date' in df.columns and 'time' in df.columns:
                    df['datetime'] = df['trade_date'].astype(str) + ' ' + df['time'].astype(str)
            
            # 5. 数据类型转换
            numeric_columns = ['price', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    if col == 'volume':
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 6. 剔除异常数据
            initial_count = len(df)
            
            # 剔除价格为0或负数的数据
            if 'price' in df.columns:
                df = df[df['price'] > 0]
            
            # 剔除成交量为负数的数据
            if 'volume' in df.columns:
                df = df[df['volume'] >= 0]
            
            # 剔除成交额为负数的数据
            if 'amount' in df.columns:
                df = df[df['amount'] >= 0]
            
            removed_count = initial_count - len(df)
            if removed_count > 0:
                logger.info(f"剔除异常数据: {removed_count} 条")
            
            # 7. 处理缺失值
            if self.transform_rules.get("fill_missing", False):
                # 对于价格数据，使用前一个时间点的价格填充
                if 'price' in df.columns:
                    df = df.sort_values(['ts_code', 'trade_date', 'time'])
                    df['price'] = df.groupby(['ts_code', 'trade_date'])['price'].ffill()
            
            # 8. 确保必需字段存在
            required_columns = ['ts_code', 'trade_date', 'time', 'price', 'volume', 'amount']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
            
            # 9. 按股票代码、日期、时间排序
            if 'ts_code' in df.columns and 'trade_date' in df.columns and 'time' in df.columns:
                df = df.sort_values(['ts_code', 'trade_date', 'time']).reset_index(drop=True)
            
            logger.info(f"转换完成，最终数据量: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"转换分时K线数据失败: {e}")
            raise TransformerException(f"转换分时K线数据失败: {e}") from e
    
    def _normalize_time(self, time_str: str) -> str:
        """
        标准化时间格式为 HH:MM:SS
        
        Args:
            time_str: 时间字符串，可能是各种格式
            
        Returns:
            str: 标准化后的时间字符串 (HH:MM:SS)
        """
        if not time_str or time_str.strip() == '':
            return None
        
        # 移除空格
        time_str = time_str.strip()
        
        # 尝试解析各种格式
        try:
            # 如果已经是 HH:MM:SS 格式
            if len(time_str) == 8 and time_str.count(':') == 2:
                parts = time_str.split(':')
                if len(parts) == 3 and all(part.isdigit() for part in parts):
                    return time_str
            
            # 如果是 HH:MM 格式，补充秒
            if len(time_str) == 5 and time_str.count(':') == 1:
                return time_str + ':00'
            
            # 如果是 HHMMSS 格式（无冒号）
            if len(time_str) == 6 and time_str.isdigit():
                return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            
            # 如果是 HHMM 格式（无冒号）
            if len(time_str) == 4 and time_str.isdigit():
                return f"{time_str[:2]}:{time_str[2:4]}:00"
            
            # 尝试使用 pandas 解析
            from datetime import datetime
            parsed_time = pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')
            if pd.notna(parsed_time):
                return parsed_time.strftime('%H:%M:%S')
            
            logger.warning(f"无法解析时间格式: {time_str}，返回原值")
            return time_str
            
        except Exception as e:
            logger.warning(f"标准化时间格式失败: {time_str}, 错误: {e}")
            return time_str

