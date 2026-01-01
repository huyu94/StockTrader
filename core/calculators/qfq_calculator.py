"""
前复权计算器

用于计算股票的前复权价格。
根据 README 中的架构设计，计算器接收 DataFrame 参数，不直接依赖 Collector/Loader。
"""

import pandas as pd
import numpy as np
from typing import Optional
from loguru import logger

from utils.date_helper import DateHelper


class QFQCalculator:
    """
    前复权计算器
    
    功能：
    1. 接收日K线数据（未复权价格）和复权因子数据
    2. 计算前复权价格
    3. 返回包含前复权价格的数据
    
    前复权计算公式：
    前复权价(历史日期T) = 未复权价(T) × 最新复权因子 / 历史复权因子(T)
    
    设计原则：
    - 接收 DataFrame 参数，无隐藏依赖
    - 可选：从 Loader 读取辅助数据（同层依赖，可接受）
    """
    
    def __init__(self, loader=None):
        """
        初始化前复权计算器
        
        Args:
            loader: 可选的 Loader 实例，用于读取复权因子数据（如果需要）
        """
        self.loader = loader
        logger.debug("初始化前复权计算器")
    
    def calculate(
        self,
        kline_df: pd.DataFrame,
        adj_factor_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        计算前复权价格
        
        前复权计算公式：前复权价(T) = 未复权价(T) × 最新复权因子 / 历史复权因子(T)
        
        Args:
            kline_df: 日K线数据DataFrame，每个交易日一行，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - open, high, low, close: 未复权价格
                - 其他列（可选）
            adj_factor_df: 复权因子DataFrame，只有复权因子变化的那天有数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期（复权因子生效日期）
                - adj_factor: 复权因子
        
        Returns:
            pd.DataFrame: 更新后的DataFrame，包含以下新增列：
                - adj_factor: 每个交易日对应的复权因子
                - close_qfq: 前复权收盘价
                - open_qfq: 前复权开盘价
                - high_qfq: 前复权最高价
                - low_qfq: 前复权最低价
        """
        if kline_df is None or kline_df.empty:
            logger.warning("日K线数据为空，无法计算前复权价格")
            return pd.DataFrame()
        
        # 复制数据，避免修改原DataFrame
        result_df = kline_df.copy()
        
        # 检查 adj_factor_df 是否为 None 或空
        if adj_factor_df is None or adj_factor_df.empty:
            logger.warning("复权因子数据为空，无法计算前复权价格")
            return result_df
        
        # 确保必需列存在
        required_kline_cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
        missing_kline_cols = [col for col in required_kline_cols if col not in result_df.columns]
        if missing_kline_cols:
            raise ValueError(f"日K线数据缺少必需的列: {missing_kline_cols}")
        
        required_adj_cols = ['ts_code', 'trade_date', 'adj_factor']
        missing_adj_cols = [col for col in required_adj_cols if col not in adj_factor_df.columns]
        if missing_adj_cols:
            raise ValueError(f"复权因子数据缺少必需的列: {missing_adj_cols}")
        
        # 确保trade_date是日期类型（统一转换为datetime）
        if not pd.api.types.is_datetime64_any_dtype(result_df['trade_date']):
            result_df['trade_date'] = pd.to_datetime(result_df['trade_date'], errors='coerce')
        
        if not pd.api.types.is_datetime64_any_dtype(adj_factor_df['trade_date']):
            adj_factor_df = adj_factor_df.copy()  # 避免修改原DataFrame
            adj_factor_df['trade_date'] = pd.to_datetime(adj_factor_df['trade_date'], errors='coerce')
        
        # 按股票代码分组处理
        result_list = []
        
        for ts_code, group_df in result_df.groupby('ts_code'):
            # 获取该股票的复权因子数据
            stock_adj_factor = adj_factor_df[adj_factor_df['ts_code'] == ts_code].copy()
            
            if stock_adj_factor.empty:
                logger.warning(f"股票 {ts_code} 没有复权因子数据，跳过前复权计算")
                result_list.append(group_df)
                continue
            
            # 按日期排序
            group_df = group_df.sort_values('trade_date').reset_index(drop=True)
            stock_adj_factor = stock_adj_factor.sort_values('trade_date').reset_index(drop=True)
            
            # 为每个交易日找到对应的复权因子（≤ 该日期的最近复权因子）
            # 使用 merge_asof 进行向前填充
            merged_df = pd.merge_asof(
                group_df,
                stock_adj_factor[['trade_date', 'adj_factor']],
                on='trade_date',
                direction='backward'  # 向后查找，找到 ≤ trade_date 的最近复权因子
            ).copy()
            
            # 检查是否有交易日早于最早的复权因子日期
            earliest_adj_date = stock_adj_factor['trade_date'].min()
            missing_dates = merged_df[merged_df['trade_date'] < earliest_adj_date]
            if not missing_dates.empty:
                logger.warning(
                    f"股票 {ts_code} 有 {len(missing_dates)} 个交易日早于最早的复权因子日期 "
                    f"{earliest_adj_date.strftime('%Y-%m-%d')}，这些交易日的前复权价格将无法计算"
                )
            
            # 获取最新复权因子（所有复权因子中日期最大的）
            latest_adj_factor = stock_adj_factor.loc[stock_adj_factor['trade_date'].idxmax(), 'adj_factor']
            if pd.isna(latest_adj_factor) or latest_adj_factor <= 0:
                logger.warning(f"股票 {ts_code} 的最新复权因子无效: {latest_adj_factor}，跳过前复权计算")
                result_list.append(group_df)
                continue
            
            # 计算前复权价格
            # 前复权价(T) = 未复权价(T) × 最新复权因子 / 历史复权因子(T)
            merged_df['ratio'] = merged_df['adj_factor'].apply(
                lambda x: latest_adj_factor / x if pd.notna(x) and x > 0 else np.nan
            )
            
            merged_df['close_qfq'] = merged_df['close'] * merged_df['ratio']
            merged_df['open_qfq'] = merged_df['open'] * merged_df['ratio']
            merged_df['high_qfq'] = merged_df['high'] * merged_df['ratio']
            merged_df['low_qfq'] = merged_df['low'] * merged_df['ratio']
            
            # 删除临时列 ratio
            merged_df = merged_df.drop(columns=['ratio'], errors='ignore')
            
            result_list.append(merged_df)
        
        # 合并所有股票的结果
        if result_list:
            final_result = pd.concat(result_list, ignore_index=True)
            logger.info(f"前复权计算完成，共处理 {len(final_result)} 条记录")
            return final_result
        else:
            logger.warning("没有成功计算前复权价格的数据")
            return result_df
    
    def calculate_from_loader(
        self,
        kline_df: pd.DataFrame,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        从 Loader 读取复权因子数据并计算前复权价格（可选功能）
        
        Args:
            kline_df: 日K线数据DataFrame
            ts_code: 股票代码（如果 kline_df 只包含一只股票，可以指定）
            start_date: 开始日期（如果未指定，从 kline_df 中获取）
            end_date: 结束日期（如果未指定，从 kline_df 中获取）
        
        Returns:
            pd.DataFrame: 包含前复权价格的数据
        """
        if self.loader is None:
            raise ValueError("未提供 Loader 实例，无法从数据库读取复权因子数据")
        
        # 如果没有指定 ts_code，从 kline_df 中获取
        if ts_code is None:
            if 'ts_code' not in kline_df.columns:
                raise ValueError("kline_df 中缺少 ts_code 列，无法确定股票代码")
            unique_codes = kline_df['ts_code'].unique()
            if len(unique_codes) > 1:
                raise ValueError(f"kline_df 包含多只股票 ({len(unique_codes)} 只)，请指定 ts_code")
            ts_code = unique_codes[0]
        
        # 如果没有指定日期范围，从 kline_df 中获取
        if start_date is None or end_date is None:
            if 'trade_date' not in kline_df.columns:
                raise ValueError("kline_df 中缺少 trade_date 列，无法确定日期范围")
            start_date = kline_df['trade_date'].min()
            end_date = kline_df['trade_date'].max()
        
        # 从 Loader 读取复权因子数据
        # 注意：这里需要根据实际的 Loader 接口调整
        # 假设 Loader 有 load_adj_factor 方法
        logger.info(f"从数据库读取股票 {ts_code} 的复权因子数据，日期范围: {start_date} ~ {end_date}")
        
        # TODO: 实现从 Loader 读取复权因子的逻辑
        # adj_factor_df = self.loader.load_adj_factor(ts_code, start_date, end_date)
        
        # 暂时返回原数据，提示需要实现
        logger.warning("calculate_from_loader 方法需要根据实际的 Loader 接口实现")
        return kline_df

