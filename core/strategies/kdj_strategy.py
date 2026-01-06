"""
少妇战法策略 (KDJ Strategy)

策略名称：少妇战法
策略逻辑：
1. KDJ指标的J值必须小于等于5（超卖信号）
2. 当前成交量必须小于前20日成交量最大值的1/2（缩量信号）
3. 根据价格运行位置，检查是否接近对应均线且未跌破：
   - 如果运行在20日线上方，检查是否接近20日线且未跌破
   - 如果运行在30日线上方，检查是否接近30日线且未跌破
   - 如果运行在60日线上方，检查是否接近60日线且未跌破

买入条件：
- 同时满足以上三个条件时，认为符合买入条件
- 这是一个寻找超卖+缩量+价格回归均线且未跌破的策略，适合捕捉反弹机会
"""

import pandas as pd
import numpy as np
from typing import Any, List, Union, Optional
from loguru import logger

from core.strategies.base import BaseStrategy
from core.calculators.indicator_calculator import IndicatorCalculator


class KDJStrategy(BaseStrategy):
    """
    少妇战法策略
    
    策略说明：
    1. KDJ指标的J值 <= 5：表示股票处于超卖状态，可能反弹
    2. 成交量缩量：当前成交量 < 前20日最大成交量的1/2，表示抛压减轻
    3. 价格回归均线且未跌破：
       - 如果运行在MA20上方，检查是否接近MA20且未跌破
       - 如果运行在MA30上方，检查是否接近MA30且未跌破
       - 如果运行在MA60上方，检查是否接近MA60且未跌破
       - 优先级：MA20 > MA30 > MA60
    
    适用场景：
    - 适合捕捉超跌反弹机会
    - 适合在震荡市中使用
    - 需要结合其他指标确认买入时机
    - 根据价格运行位置动态调整均线检查，更灵活
    """
    
    def __init__(
        self, 
        kdj_period: int = 9, 
        vol_period: int = 20, 
        j_threshold: float = 5.0,
        ma_tolerance: float = 0.03,
        name: Optional[str] = None
    ):
        """
        初始化策略
        
        Args:
            kdj_period: KDJ指标计算周期，默认9
            vol_period: 成交量比较周期，默认20
            j_threshold: J值阈值，默认5.0（J值必须小于等于此值）
            ma_tolerance: 接近均线的容忍度，默认0.03（3%），表示价格在均线的±3%范围内算接近
            name: 策略名称，默认"少妇战法"
        """
        super().__init__(name=name or "少妇战法")
        self.kdj_period = kdj_period
        self.vol_period = vol_period
        self.j_threshold = j_threshold
        self.ma_tolerance = ma_tolerance
        self.indicator_calculator = IndicatorCalculator()
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标
        
        计算KDJ指标、均线指标和成交量相关指标
        
        Args:
            df: 股票K线数据DataFrame
        
        Returns:
            pd.DataFrame: 添加了技术指标列后的DataFrame
        """
        if df is None or df.empty:
            logger.warning("数据为空，无法计算指标")
            return df
        
        
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if 'trade_date' in df_copy.columns:
            df_copy = df_copy.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
        
        # 按股票代码分组处理
        result_list = []
        
        for ts_code, group_df in df_copy.groupby('ts_code'):
            group_df = group_df.sort_values('trade_date').reset_index(drop=True)
            
            # 确保数值列为float类型（处理数据库返回的Decimal类型）
            numeric_columns = ['open', 'high', 'low', 'close', 'vol', 'amount']
            for col in numeric_columns:
                if col in group_df.columns:
                    group_df[col] = pd.to_numeric(group_df[col], errors='coerce').astype(float)
            
            # 检查 close 列（聚合器已经将close_qfq复制到close，并删除了_qfq后缀列）
            if 'close' not in group_df.columns:
                continue
            
            close_values = group_df['close'].values
            
            # 1. 计算KDJ指标
            group_df = self.indicator_calculator.calculate_kdj(
                group_df, 
                period=self.kdj_period
            )
            
            # 2. 计算均线指标（MA20, MA30, MA60）
            
            group_df = self.indicator_calculator.calculate_ma(
                group_df, 
                periods=[20, 30, 60], 
                column='close'
            )
            
            # 检查计算后的均线值
            latest_row = group_df.iloc[-1] if len(group_df) > 0 else None
            if latest_row is not None:
                ma20_val = latest_row.get('ma20', 'N/A')
                ma30_val = latest_row.get('ma30', 'N/A')
                ma60_val = latest_row.get('ma60', 'N/A')
                latest_close = latest_row.get('close', 'N/A')
                latest_date = latest_row.get('trade_date', 'N/A')

            
            # 3. 计算成交量相关指标
            # 计算前N日的成交量最大值
            group_df['vol_max_20'] = group_df['vol'].rolling(window=self.vol_period).max()
            # 计算当前成交量与最大值的比例
            group_df['vol_ratio'] = np.where(
                group_df['vol_max_20'] > 0,
                group_df['vol'] / group_df['vol_max_20'],
                1.0  # 如果最大成交量为0，比例设为1
            )
            
            # 标记是否满足成交量缩量条件（当前成交量 < 最大成交量的1/2）
            group_df['vol_shrink'] = group_df['vol_ratio'] < 0.5
            
            # 标记是否满足J值条件（J值 <= 阈值）
            group_df['kdj_oversold'] = group_df['kdj_j'] <= self.j_threshold
            
            # 4. 计算均线相关指标
            # 判断收盘价在哪个均线上方（优先级：MA20 > MA30 > MA60）
            group_df['above_ma20'] = group_df['close'] > group_df['ma20']
            group_df['above_ma30'] = group_df['close'] > group_df['ma30']
            group_df['above_ma60'] = group_df['close'] > group_df['ma60']
            
            # 计算收盘价与各均线的偏离度
            group_df['close_to_ma20'] = np.where(
                group_df['ma20'] > 0,
                (group_df['close'] - group_df['ma20']) / group_df['ma20'],
                np.nan
            )
            group_df['close_to_ma30'] = np.where(
                group_df['ma30'] > 0,
                (group_df['close'] - group_df['ma30']) / group_df['ma30'],
                np.nan
            )
            group_df['close_to_ma60'] = np.where(
                group_df['ma60'] > 0,
                (group_df['close'] - group_df['ma60']) / group_df['ma60'],
                np.nan
            )
            
            # 判断是否接近各均线（在容忍度范围内，且在上方）
            # 接近MA20：在MA20上方 且 偏离度 <= tolerance
            group_df['near_ma20'] = (
                group_df['above_ma20'] & 
                (group_df['close_to_ma20'] <= self.ma_tolerance) &
                (group_df['close_to_ma20'] >= 0)  # 确保在上方，不能跌破
            )
            
            # 接近MA30：在MA30上方 且 偏离度 <= tolerance（且不在MA20上方，因为优先级更高）
            group_df['near_ma30'] = (
                group_df['above_ma30'] & 
                ~group_df['above_ma20'] &  # 不在MA20上方（因为如果在上方，应该检查MA20）
                (group_df['close_to_ma30'] <= self.ma_tolerance) &
                (group_df['close_to_ma30'] >= 0)  # 确保在上方，不能跌破
            )
            
            # 接近MA60：在MA60上方 且 偏离度 <= tolerance（且不在MA20和MA30上方）
            group_df['near_ma60'] = (
                group_df['above_ma60'] & 
                ~group_df['above_ma20'] &  # 不在MA20上方
                ~group_df['above_ma30'] &  # 不在MA30上方
                (group_df['close_to_ma60'] <= self.ma_tolerance) &
                (group_df['close_to_ma60'] >= 0)  # 确保在上方，不能跌破
            )
            
            # 判断是否满足条件：接近任一均线且未跌破
            group_df['near_ma'] = group_df['near_ma20'] | group_df['near_ma30'] | group_df['near_ma60']
            
            # 记录当前价格运行在哪个均线上方（用于日志）
            group_df['running_ma'] = np.where(
                group_df['above_ma20'], 'MA20',
                np.where(
                    group_df['above_ma30'], 'MA30',
                    np.where(group_df['above_ma60'], 'MA60', 'None')
                )
            )
            
            result_list.append(group_df)
        
        # 合并所有股票的结果
        if result_list:
            result_df = pd.concat(result_list, ignore_index=True)
            return result_df
        else:
            return df_copy
    
    def filter_stocks(self, df: pd.DataFrame) -> Union[List[str], pd.DataFrame]:
        """
        根据策略规则筛选股票
        
        买入条件：
        1. KDJ的J值 <= 阈值（超卖信号）
        2. 当前成交量 < 前20日最大成交量的1/2（缩量信号）
        3. 收盘价接近MA20或MA30或MA60且未跌破（价格回归均线）
        
        Args:
            df: 包含技术指标的K线数据DataFrame
        
        Returns:
            pd.DataFrame: 包含筛选结果的DataFrame，包含ts_code、trade_date等列
        """
        if df is None or df.empty:
            logger.warning("数据为空，无法筛选股票")
            return pd.DataFrame(columns=['ts_code', 'trade_date'])
        
        # 需要至少60个交易日的数据来计算所有指标
        min_period = max(60, self.vol_period, self.kdj_period)
        
        result_list = []
        debug_stats = {
            'total_stocks': len(df.groupby('ts_code')) if 'ts_code' in df.columns else 0,
            'insufficient_data': 0,
            'missing_indicators': 0,
            'condition1_fail': 0,
            'condition2_fail': 0,
            'condition3_fail': 0,
            'passed': 0
        }
        
        # 按股票代码分组处理
        for ts_code, group_df in df.groupby('ts_code'):
            group_df = group_df.sort_values('trade_date').reset_index(drop=True)
            
            # 检查数据量是否足够
            if len(group_df) < min_period:
                debug_stats['insufficient_data'] += 1
                continue
            
            # 获取最新交易日的数据
            latest_row = group_df.iloc[-1]
            latest_date = latest_row.get('trade_date', 'Unknown')
            
            # 检查数据有效性 - 详细记录缺失的指标
            missing_indicators = []
            if pd.isna(latest_row.get('kdj_j')):
                missing_indicators.append('kdj_j')
            if pd.isna(latest_row.get('vol')):
                missing_indicators.append('vol')
            if pd.isna(latest_row.get('vol_max_20')):
                missing_indicators.append('vol_max_20')
            if pd.isna(latest_row.get('ma20')):
                missing_indicators.append('ma20')
            if pd.isna(latest_row.get('ma30')):
                missing_indicators.append('ma30')
            if pd.isna(latest_row.get('ma60')):
                missing_indicators.append('ma60')
            
            if missing_indicators:
                debug_stats['missing_indicators'] += 1
                continue
            
            # 条件1：KDJ的J值 <= 阈值（超卖信号）
            j_value = latest_row.get('kdj_j', 999)
            condition1 = j_value <= self.j_threshold
            
            # 条件2：当前成交量 < 前20日最大成交量的1/2（缩量信号）
            vol_ratio = latest_row.get('vol_ratio', 1.0)
            condition2 = vol_ratio < 0.5
            
            # 条件3：根据价格运行位置，检查是否接近对应均线且未跌破
            near_ma = latest_row.get('near_ma', False)
            condition3 = bool(near_ma) if not pd.isna(near_ma) else False
            
            # 记录条件检查结果
            if not condition1:
                debug_stats['condition1_fail'] += 1
            if not condition2:
                debug_stats['condition2_fail'] += 1
            if not condition3:
                debug_stats['condition3_fail'] += 1
            
            # 三个条件都必须满足
            if condition1 and condition2 and condition3:
                debug_stats['passed'] += 1
                # 获取详细信息用于输出
                close_price = latest_row.get('close', 0)
                ma20 = latest_row.get('ma20', 0)
                ma30 = latest_row.get('ma30', 0)
                ma60 = latest_row.get('ma60', 0)
                near_ma20 = latest_row.get('near_ma20', False)
                near_ma30 = latest_row.get('near_ma30', False)
                near_ma60 = latest_row.get('near_ma60', False)
                
                # 构建均线信息
                ma_info = []
                if near_ma20:
                    ma_info.append(f"接近MA20({ma20:.2f})")
                elif near_ma30:
                    ma_info.append(f"接近MA30({ma30:.2f})")
                elif near_ma60:
                    ma_info.append(f"接近MA60({ma60:.2f})")
                
                # 符合条件的股票信息会在最终结果中显示，这里不输出日志
                
                # 添加到结果中
                result_list.append({
                    'ts_code': ts_code,
                    'trade_date': latest_row.get('trade_date'),
                    'close': close_price,
                    'kdj_j': j_value,
                    'vol_ratio': vol_ratio,
                    'ma20': ma20,
                    'ma30': ma30,
                    'ma60': ma60,
                    'near_ma': ', '.join(ma_info) if ma_info else 'None'
                })
        
        
        if result_list:
            result_df = pd.DataFrame(result_list)
            return result_df
        else:
            return pd.DataFrame(columns=['ts_code', 'trade_date'])

