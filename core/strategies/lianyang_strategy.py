"""
连阳战法策略

策略条件：
1. 不包含北交所
2. 去除ST股票
3. 去除创业板块
4. 去除科创板块
5. 连续5天以上是阳线（涨停除外）
6. 25元以下
7. 市值小于300亿元
8. 市盈率小于166
9. 换手在5以上
10. 连续五天涨幅不大于百分之十
11. 一年涨停两次以上
12. 十日新高
"""

import pandas as pd
import numpy as np
from typing import List, Union, Optional
from loguru import logger
from datetime import datetime, timedelta

from core.strategies.base import BaseStrategy
from core.loaders.basic_info import BasicInfoLoader
from core.common.exceptions import LoaderException
from utils.date_helper import DateHelper


class LianyangStrategy(BaseStrategy):
    """
    连阳战法策略
    
    策略说明：
    寻找连续多日上涨但涨幅不大的股票，结合基本面指标筛选
    """
    
    def __init__(
        self,
        min_consecutive_days: int = 5,
        max_price: float = 25.0,
        max_market_cap: float = 300.0,  # 亿元
        max_pe: float = 166.0,
        min_turnover: float = 5.0,  # 换手率
        max_5day_pct: float = 10.0,  # 连续五天涨幅不大于10%
        min_limit_up_count: int = 2,  # 一年涨停次数
        name: Optional[str] = None
    ):
        """
        初始化策略
        
        Args:
            min_consecutive_days: 最少连续阳线天数，默认5
            max_price: 最大价格，默认25元
            max_market_cap: 最大市值（亿元），默认300
            max_pe: 最大市盈率，默认166
            min_turnover: 最小换手率，默认5%
            max_5day_pct: 连续五天最大涨幅，默认10%
            min_limit_up_count: 一年最少涨停次数，默认2
            name: 策略名称，默认"连阳战法"
        """
        super().__init__(name=name or "连阳战法")
        self.min_consecutive_days = min_consecutive_days
        self.max_price = max_price
        self.max_market_cap = max_market_cap
        self.max_pe = max_pe
        self.min_turnover = min_turnover
        self.max_5day_pct = max_5day_pct
        self.min_limit_up_count = min_limit_up_count
        self.basic_info_loader = BasicInfoLoader()
    
    def _is_limit_up(self, row: pd.Series, prev_close: float) -> bool:
        """
        判断是否涨停
        
        Args:
            row: 当前行数据
            prev_close: 前一日收盘价
            
        Returns:
            bool: 是否涨停
        """
        if pd.isna(prev_close) or prev_close <= 0:
            return False
        
        close = row.get('close', 0)
        if pd.isna(close) or close <= 0:
            return False
        
        # 计算涨幅
        pct_change = (close - prev_close) / prev_close * 100
        
        # 判断是否涨停（涨幅接近9.8%以上，考虑误差）
        # 主板、中小板：10%，创业板、科创板：20%，ST：5%
        ts_code = row.get('ts_code', '')
        is_st = 'ST' in str(row.get('name', ''))
        
        if is_st:
            limit_pct = 5.0
        elif ts_code.startswith('688') or ts_code.startswith('300'):
            limit_pct = 20.0
        else:
            limit_pct = 10.0
        
        # 允许0.1%的误差
        return pct_change >= limit_pct - 0.1
    
    def _is_positive_line(self, row: pd.Series, prev_close: float) -> bool:
        """
        判断是否是阳线
        
        Args:
            row: 当前行数据
            prev_close: 前一日收盘价
            
        Returns:
            bool: 是否是阳线
        """
        if pd.isna(prev_close) or prev_close <= 0:
            return False
        
        close = row.get('close', 0)
        if pd.isna(close) or close <= 0:
            return False
        
        # 阳线：收盘价 > 前一日收盘价
        return close > prev_close
    
    def _get_stock_basic_info(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        从数据库获取股票基本信息
        
        Args:
            ts_codes: 股票代码列表
            
        Returns:
            pd.DataFrame: 股票基本信息
            
        Raises:
            LoaderException: 当无法获取数据时抛出异常
        """
        df = self.basic_info_loader.read(ts_codes=ts_codes)
        if df is None or df.empty:
            raise LoaderException(f"无法从数据库获取股票基本信息，股票代码: {ts_codes}")
        return df
    
    def _get_daily_basic_data(self, ts_codes: List[str], trade_date: str) -> pd.DataFrame:
        """
        获取每日基本面数据（市值、市盈率、换手率）
        
        Args:
            ts_codes: 股票代码列表
            trade_date: 交易日期
            
        Returns:
            pd.DataFrame: 每日基本面数据
            
        Raises:
            LoaderException: 当无法获取数据时抛出异常
        """
        # 注意：daily_basic 数据目前没有对应的 Loader
        # 策略不应该直接调用 collector，应该使用 loader 从数据库获取数据
        # 如果数据库中没有 daily_basic 数据，需要先通过 pipeline 采集并存储
        raise LoaderException(
            f"无法获取每日基本面数据：daily_basic 数据目前没有对应的 Loader。"
            f"请先通过 pipeline 采集并存储 daily_basic 数据到数据库，然后创建对应的 Loader。"
            f"交易日期: {trade_date}, 股票代码数量: {len(ts_codes)}"
        )
    
    def _count_limit_up_days(self, df: pd.DataFrame, days: int = 250) -> int:
        """
        统计一年内的涨停次数
        
        Args:
            df: 股票K线数据
            days: 统计天数，默认250（约一年）
            
        Returns:
            int: 涨停次数
        """
        if df is None or df.empty or len(df) < 2:
            return 0
        
        df_sorted = df.sort_values('trade_date').reset_index(drop=True)
        # 只统计最近days天的数据
        if len(df_sorted) > days:
            df_sorted = df_sorted.tail(days)
        
        count = 0
        for i in range(1, len(df_sorted)):
            prev_row = df_sorted.iloc[i-1]
            curr_row = df_sorted.iloc[i]
            prev_close = prev_row.get('close', 0)
            
            if self._is_limit_up(curr_row, prev_close):
                count += 1
        
        return count
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标
        
        Args:
            df: 股票K线数据DataFrame
            
        Returns:
            pd.DataFrame: 添加了技术指标列后的DataFrame
        """
        if df is None or df.empty:
            return df
        
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if 'trade_date' in df_copy.columns:
            df_copy = df_copy.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
        
        result_list = []
        
        # 按股票代码分组处理
        for ts_code, group_df in df_copy.groupby('ts_code'):
            group_df = group_df.sort_values('trade_date').reset_index(drop=True)
            
            # 计算连续阳线天数（排除涨停）
            consecutive_positive = []
            for i in range(len(group_df)):
                if i == 0:
                    consecutive_positive.append(0)
                else:
                    prev_row = group_df.iloc[i-1]
                    curr_row = group_df.iloc[i]
                    prev_close = prev_row.get('close', 0)
                    
                    # 判断是否是阳线且不是涨停
                    is_positive = self._is_positive_line(curr_row, prev_close)
                    is_limit = self._is_limit_up(curr_row, prev_close)
                    
                    if is_positive and not is_limit:
                        # 如果前一天也是连续阳线，则累加
                        if consecutive_positive and consecutive_positive[-1] > 0:
                            consecutive_positive.append(consecutive_positive[-1] + 1)
                        else:
                            consecutive_positive.append(1)
                    else:
                        consecutive_positive.append(0)
            
            group_df['consecutive_positive'] = consecutive_positive
            
            # 计算5日涨幅
            group_df['pct_change_5d'] = group_df['close'].pct_change(5) * 100
            
            # 计算10日最高价（不包括当天，使用shift(1)排除当天，然后计算过去10日的最高价）
            # 先shift(1)排除当天，然后rolling计算过去10日的最高价
            group_df['high_10d'] = group_df['high'].shift(1).rolling(window=10, min_periods=1).max()
            
            # 判断是否是10日新高（当前收盘价 >= 过去10日的最高价）
            # 如果high_10d是NaN（数据不足），则不算新高
            group_df['is_10d_high'] = (group_df['close'] >= group_df['high_10d']) & (group_df['high_10d'].notna())
            
            result_list.append(group_df)
        
        if result_list:
            result_df = pd.concat(result_list, ignore_index=True)
            return result_df
        else:
            return df_copy
    
    def filter_stocks(self, df: pd.DataFrame) -> Union[List[str], pd.DataFrame]:
        """
        根据策略规则筛选股票
        
        Args:
            df: 包含技术指标的K线数据DataFrame
            
        Returns:
            pd.DataFrame: 包含筛选结果的DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=['ts_code', 'trade_date'])
        
        # 获取所有股票代码
        ts_codes = df['ts_code'].unique().tolist()
        
        # 获取股票基本信息
        basic_info_df = self._get_stock_basic_info(ts_codes)
        
        # 获取最新交易日期
        latest_date = df['trade_date'].max()
        if isinstance(latest_date, pd.Timestamp):
            trade_date_str = latest_date.strftime('%Y-%m-%d')
        else:
            trade_date_str = str(latest_date)
        
        # 获取每日基本面数据
        daily_basic_df = self._get_daily_basic_data(ts_codes, trade_date_str)
        
        result_list = []
        
        # 按股票代码分组处理
        for ts_code, group_df in df.groupby('ts_code'):
            group_df = group_df.sort_values('trade_date').reset_index(drop=True)
            
            if len(group_df) < self.min_consecutive_days + 10:  # 至少需要足够的数据
                continue
            
            # 获取最新交易日的数据
            latest_row = group_df.iloc[-1]
            
            # 条件1: 不包含北交所（.BJ结尾）
            if ts_code.endswith('.BJ'):
                continue
            
            # 条件2: 去除ST股票
            stock_name = ''
            if not basic_info_df.empty:
                stock_info = basic_info_df[basic_info_df['ts_code'] == ts_code]
                if not stock_info.empty:
                    stock_name = str(stock_info.iloc[0].get('name', ''))
                    if 'ST' in stock_name:
                        continue
            
            # 条件3: 去除创业板块（300开头或market为创业板）
            if ts_code.startswith('300'):
                continue
            if not basic_info_df.empty:
                stock_info = basic_info_df[basic_info_df['ts_code'] == ts_code]
                if not stock_info.empty:
                    market = str(stock_info.iloc[0].get('market', ''))
                    if '创业板' in market:
                        continue
            
            # 条件4: 去除科创板块（688开头或market为科创板）
            if ts_code.startswith('688'):
                continue
            if not basic_info_df.empty:
                stock_info = basic_info_df[basic_info_df['ts_code'] == ts_code]
                if not stock_info.empty:
                    market = str(stock_info.iloc[0].get('market', ''))
                    if '科创板' in market:
                        continue
            
            # 条件5: 连续5天以上是阳线（涨停除外）
            consecutive_positive = latest_row.get('consecutive_positive', 0)
            if consecutive_positive < self.min_consecutive_days:
                continue
            
            # 条件6: 25元以下
            close_price = latest_row.get('close', 0)
            if pd.isna(close_price) or close_price >= self.max_price:
                continue
            
            # 条件7: 市值小于300亿元
            total_mv = 0
            if not daily_basic_df.empty:
                basic_data = daily_basic_df[daily_basic_df['ts_code'] == ts_code]
                if not basic_data.empty:
                    total_mv = basic_data.iloc[0].get('total_mv', 0)
                    if pd.notna(total_mv) and total_mv > 0:
                        total_mv = total_mv / 10000  # 转换为亿元
            if total_mv > 0 and total_mv >= self.max_market_cap:
                continue
            
            # 条件8: 市盈率小于166
            pe = 0
            if not daily_basic_df.empty:
                basic_data = daily_basic_df[daily_basic_df['ts_code'] == ts_code]
                if not basic_data.empty:
                    pe = basic_data.iloc[0].get('pe', 0)
            if pe > 0 and (pd.isna(pe) or pe >= self.max_pe):
                continue
            
            # 条件9: 换手在5以上
            turnover = 0
            if not daily_basic_df.empty:
                basic_data = daily_basic_df[daily_basic_df['ts_code'] == ts_code]
                if not basic_data.empty:
                    turnover = basic_data.iloc[0].get('turnover_rate', 0)
            if pd.isna(turnover) or turnover < self.min_turnover:
                continue
            
            # 条件10: 连续五天涨幅不大于百分之十
            pct_change_5d = latest_row.get('pct_change_5d', 0)
            if pd.isna(pct_change_5d) or pct_change_5d > self.max_5day_pct:
                continue
            
            # 条件11: 一年涨停两次以上
            limit_up_count = self._count_limit_up_days(group_df)
            if limit_up_count < self.min_limit_up_count:
                continue
            
            # 条件12: 十日新高
            is_10d_high = latest_row.get('is_10d_high', False)
            if not is_10d_high:
                continue
            
            # 所有条件都满足，添加到结果
            result_list.append({
                'ts_code': ts_code,
                'trade_date': latest_row.get('trade_date'),
                'close': close_price,
                'consecutive_positive': consecutive_positive,
                'total_mv': total_mv,
                'pe': pe,
                'turnover': turnover,
                'pct_change_5d': pct_change_5d,
                'limit_up_count': limit_up_count
            })
        
        if result_list:
            result_df = pd.DataFrame(result_list)
            return result_df
        else:
            return pd.DataFrame(columns=['ts_code', 'trade_date'])

