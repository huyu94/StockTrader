"""
交易日历转换器

负责清洗、标准化交易日历数据
"""

from typing import Any, Dict
import pandas as pd
from loguru import logger

from core.transformers.base import BaseTransformer
from core.common.exceptions import TransformerException
from utils.date_helper import DateHelper


class TradeCalendarTransformer(BaseTransformer):
    """
    交易日历转换器
    
    对采集到的交易日历数据进行清洗、标准化处理：
    - 字段重命名和映射
    - 数据类型转换
    - 日期格式标准化
    - 数据去重和排序
    - 将长格式（每行一个交易所）转换为宽格式（每行一个日期，包含所有交易所状态）
    """
    
    # 交易所代码到模型字段的映射
    EXCHANGE_MAPPING = {
        'SSE': 'sse_open',
        'SZSE': 'szse_open',
        'CFFEX': 'cffex_open',
        'SHFE': 'shfe_open',
        'CZCE': 'czce_open',
        'DCE': 'dce_open',
        'INE': 'ine_open',
    }
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        转换交易日历数据
        
        将 TradeCalendarCollector 返回的长格式数据（每行一个交易所）转换为
        TradeCalendar 模型需要的宽格式（每行一个日期，包含所有交易所状态）
        
        Args:
            data: 原始交易日历数据（包含 exchange, cal_date, is_open 列）
            
        Returns:
            pd.DataFrame: 转换后的交易日历数据，包含以下列：
                - cal_date: 日历日期 (YYYY-MM-DD)
                - sse_open: SSE是否交易 (bool)
                - szse_open: SZSE是否交易 (bool)
                - cffex_open: CFFEX是否交易 (bool)
                - shfe_open: SHFE是否交易 (bool)
                - czce_open: CZCE是否交易 (bool)
                - dce_open: DCE是否交易 (bool)
                - ine_open: INE是否交易 (bool)
            
        Raises:
            TransformerException: 转换失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("输入数据为空，返回空 DataFrame")
            return pd.DataFrame(columns=['cal_date', 'sse_open', 'szse_open', 'cffex_open', 
                                         'shfe_open', 'czce_open', 'dce_open', 'ine_open'])
        
        
        try:
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 1. 字段重命名（如果需要）
            column_mapping = self.transform_rules.get("column_mapping", {})
            if column_mapping:
                df = self._rename_columns(df, column_mapping)
            
            # 2. 确保必需字段存在
            required_columns = ['exchange', 'cal_date', 'is_open']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
            
            # 3. 标准化日期格式
            df['cal_date'] = df['cal_date'].apply(
                lambda x: DateHelper.normalize_to_yyyy_mm_dd(str(x)) if pd.notna(x) else None
            )
            
            # 4. 数据类型转换：将 is_open 转换为布尔值
            df['is_open'] = pd.to_numeric(df['is_open'], errors='coerce').fillna(0).astype(int)
            df['is_open'] = df['is_open'].clip(0, 1).astype(bool)  # 转换为布尔值
            
            # 5. 数据去重（基于 exchange 和 cal_date）
            initial_count = len(df)
            df = df.drop_duplicates(subset=['exchange', 'cal_date'], keep='last')
            removed_count = initial_count - len(df)
            if removed_count > 0:
                logger.info(f"去除重复数据: {removed_count} 条")
            
            # 6. 将长格式转换为宽格式（按日期聚合，每个交易所作为一列）
            # 使用 pivot_table 将 exchange 列转换为多个列
            pivot_df = df.pivot_table(
                index='cal_date',
                columns='exchange',
                values='is_open',
                aggfunc='first'  # 如果同一日期同一交易所有多条记录，取第一条
            )
            
            # 7. 重命名列为模型需要的格式（{exchange}_open）
            # 只处理已知的交易所
            result_df = pd.DataFrame(index=pivot_df.index)
            result_df['cal_date'] = result_df.index
            
            # 为每个已知交易所创建列
            for exchange_code, column_name in self.EXCHANGE_MAPPING.items():
                if exchange_code in pivot_df.columns:
                    # 先转换为布尔值，缺失值填充为 False
                    series = pivot_df[exchange_code]
                    result_df[column_name] = series.where(pd.notna(series), False).astype(bool)
                else:
                    # 如果该交易所没有数据，默认为 False
                    result_df[column_name] = False
            
            # 8. 重置索引，使 cal_date 成为普通列
            result_df = result_df.reset_index(drop=True)
            
            # 9. 按日期排序
            result_df = result_df.sort_values('cal_date').reset_index(drop=True)
            
            # 10. 确保 cal_date 是字符串格式
            result_df['cal_date'] = result_df['cal_date'].astype(str)
            
            logger.debug(f"转换完成，最终数据量: {len(result_df)} 条（从 {len(df)} 条长格式数据转换）")
            return result_df
            
        except Exception as e:
            logger.error(f"转换交易日历失败: {e}")
            raise TransformerException(f"转换交易日历失败: {e}") from e

