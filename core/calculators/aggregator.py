"""
聚合计算器

用于将分时数据聚合为日K线数据
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from loguru import logger

from utils.date_helper import DateHelper


class Aggregator:
    """
    聚合计算器
    
    功能：
    1. 将分时数据聚合为日K线数据
    2. 支持多种聚合方式（开高低收、成交量、成交额等）
    
    聚合规则：
    - 开盘价：当日第一笔交易价格
    - 收盘价：当日最后一笔交易价格
    - 最高价：当日所有分时数据的最高价
    - 最低价：当日所有分时数据的最低价
    - 成交量：当日所有分时数据的成交量之和
    - 成交额：当日所有分时数据的成交额之和
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化聚合计算器
        
        Args:
            config: 配置字典，包含聚合规则等配置
        """
        self.config = config or {}
        logger.debug("初始化聚合计算器")
    
    def aggregate_to_daily(self, intraday_df: pd.DataFrame) -> pd.DataFrame:
        """
        将分时数据聚合为日K线数据
        
        Args:
            intraday_df: 分时数据DataFrame，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - time: 时间 (HH:MM:SS)
                - price: 价格
                - volume: 成交量
                - amount: 成交额
        
        Returns:
            pd.DataFrame: 日K线数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - open: 开盘价（当日第一笔交易价格）
                - close: 收盘价（当日最后一笔交易价格）
                - high: 最高价（当日最高价格）
                - low: 最低价（当日最低价格）
                - vol: 成交量（当日成交量之和）
                - amount: 成交额（当日成交额之和）
        """
        if intraday_df is None or intraday_df.empty:
            logger.warning("分时数据为空，无法聚合")
            return pd.DataFrame()
        
        # 检查必需列
        required_columns = ['ts_code', 'trade_date', 'time', 'price', 'volume', 'amount']
        missing_columns = [col for col in required_columns if col not in intraday_df.columns]
        if missing_columns:
            raise ValueError(f"分时数据缺少必需的列: {missing_columns}")
        
        logger.info(f"开始聚合分时数据为日K线，输入数据量: {len(intraday_df)}")
        
        # 确保按时间排序
        intraday_df = intraday_df.sort_values(['ts_code', 'trade_date', 'time']).reset_index(drop=True)
        
        # 按股票代码和交易日期分组聚合
        daily_data = []
        
        for (ts_code, trade_date), group in intraday_df.groupby(['ts_code', 'trade_date']):
            # 开盘价：第一笔交易价格
            open_price = group.iloc[0]['price'] if len(group) > 0 else None
            
            # 收盘价：最后一笔交易价格
            close_price = group.iloc[-1]['price'] if len(group) > 0 else None
            
            # 最高价：所有价格中的最大值
            high_price = group['price'].max() if len(group) > 0 else None
            
            # 最低价：所有价格中的最小值
            low_price = group['price'].min() if len(group) > 0 else None
            
            # 成交量：所有成交量之和
            total_volume = group['volume'].sum() if len(group) > 0 else 0
            
            # 成交额：所有成交额之和
            total_amount = group['amount'].sum() if len(group) > 0 else 0.0
            
            # 验证数据有效性
            if pd.isna(open_price) or pd.isna(close_price) or pd.isna(high_price) or pd.isna(low_price):
                logger.warning(f"股票 {ts_code} 在日期 {trade_date} 的价格数据不完整，跳过")
                continue
            
            # 验证 OHLC 关系
            if high_price < low_price or high_price < open_price or high_price < close_price:
                logger.warning(f"股票 {ts_code} 在日期 {trade_date} 的 OHLC 关系异常，跳过")
                continue
            
            if low_price > open_price or low_price > close_price:
                logger.warning(f"股票 {ts_code} 在日期 {trade_date} 的 OHLC 关系异常，跳过")
                continue
            
            daily_data.append({
                'ts_code': ts_code,
                'trade_date': trade_date,
                'open': float(open_price),
                'high': float(high_price),
                'low': float(low_price),
                'close': float(close_price),
                'vol': int(total_volume) if pd.notna(total_volume) else 0,
                'amount': float(total_amount) if pd.notna(total_amount) else 0.0,
            })
        
        if not daily_data:
            logger.warning("聚合后没有有效数据")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(daily_data)
        
        # 确保日期格式标准化
        if 'trade_date' in result_df.columns:
            # 使用 parse_to_str 处理 datetime、date 和字符串类型
            result_df['trade_date'] = result_df['trade_date'].apply(
                lambda x: DateHelper.parse_to_str(x) if pd.notna(x) else None
            )
        
        # 按股票代码和日期排序
        result_df = result_df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
        
        logger.info(f"聚合完成，共生成 {len(result_df)} 条日K线数据")
        return result_df
    
    def aggregate_with_custom_rules(
        self,
        intraday_df: pd.DataFrame,
        aggregation_rules: Dict[str, str]
    ) -> pd.DataFrame:
        """
        使用自定义规则聚合分时数据
        
        Args:
            intraday_df: 分时数据DataFrame
            aggregation_rules: 聚合规则字典，格式如：
                {
                    'open': 'first',  # 开盘价使用第一个值
                    'close': 'last',  # 收盘价使用最后一个值
                    'high': 'max',    # 最高价使用最大值
                    'low': 'min',     # 最低价使用最小值
                    'volume': 'sum',  # 成交量使用求和
                    'amount': 'sum'   # 成交额使用求和
                }
        
        Returns:
            pd.DataFrame: 聚合后的日K线数据
        """
        if intraday_df is None or intraday_df.empty:
            logger.warning("分时数据为空，无法聚合")
            return pd.DataFrame()
        
        # 确保按时间排序
        intraday_df = intraday_df.sort_values(['ts_code', 'trade_date', 'time']).reset_index(drop=True)
        
        # 默认聚合规则
        default_rules = {
            'open': 'first',
            'close': 'last',
            'high': 'max',
            'low': 'min',
            'volume': 'sum',
            'amount': 'sum'
        }
        
        # 合并用户规则和默认规则
        rules = {**default_rules, **aggregation_rules}
        
        # 构建聚合字典
        agg_dict = {}
        for col, rule in rules.items():
            if col in intraday_df.columns:
                if rule == 'first':
                    # 使用自定义函数
                    def first_func(x):
                        return x.iloc[0] if len(x) > 0 else None
                    agg_dict[col] = first_func
                elif rule == 'last':
                    # 使用自定义函数
                    def last_func(x):
                        return x.iloc[-1] if len(x) > 0 else None
                    agg_dict[col] = last_func
                elif rule == 'max':
                    agg_dict[col] = 'max'
                elif rule == 'min':
                    agg_dict[col] = 'min'
                elif rule == 'sum':
                    agg_dict[col] = 'sum'
                elif rule == 'mean':
                    agg_dict[col] = 'mean'
                else:
                    logger.warning(f"不支持的聚合规则: {rule}，使用默认规则 first")
                    def first_func_default(x):
                        return x.iloc[0] if len(x) > 0 else None
                    agg_dict[col] = first_func_default
        
        # 执行聚合
        result_df = intraday_df.groupby(['ts_code', 'trade_date']).agg(agg_dict).reset_index()
        
        # 重命名列（如果需要）
        column_mapping = {
            'price': 'close',  # 如果使用 price 列，默认作为收盘价
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in result_df.columns and new_col not in result_df.columns:
                result_df[new_col] = result_df[old_col]
        
        # 确保日期格式标准化
        if 'trade_date' in result_df.columns:
            # 使用 parse_to_str 处理 datetime、date 和字符串类型
            result_df['trade_date'] = result_df['trade_date'].apply(
                lambda x: DateHelper.parse_to_str(x) if pd.notna(x) else None
            )
        
        logger.info(f"自定义规则聚合完成，共生成 {len(result_df)} 条日K线数据")
        return result_df

