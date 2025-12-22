"""
少妇战法策略 (KDJ Strategy)

策略名称：少妇战法
策略逻辑：
1. KDJ指标的J值必须小于等于5（超卖信号）
2. 当前成交量必须小于前20日成交量最大值的1/2（缩量信号）

买入条件：
- 同时满足以上两个条件时，认为符合买入条件
- 这是一个寻找超卖+缩量的策略，适合捕捉反弹机会
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
from .base_strategy import BaseStrategy
from loguru import logger


class KDJStrategy(BaseStrategy):
    """
    少妇战法策略
    
    策略说明：
    1. KDJ指标的J值 <= 5：表示股票处于超卖状态，可能反弹
    2. 成交量缩量：当前成交量 < 前20日最大成交量的1/2，表示抛压减轻
    
    适用场景：
    - 适合捕捉超跌反弹机会
    - 适合在震荡市中使用
    - 需要结合其他指标确认买入时机
    """
    
    def __init__(self, storage=None, kdj_period: int = 9, vol_period: int = 20, j_threshold: float = 5.0):
        """
        初始化策略
        
        :param storage: 日线数据存储对象（DailyKlineStorageSQLite实例）
        :param kdj_period: KDJ指标计算周期，默认9
        :param vol_period: 成交量比较周期，默认20
        :param j_threshold: J值阈值，默认5.0（J值必须小于等于此值）
        """
        super().__init__(storage=storage, name="少妇战法")
        self.kdj_period = kdj_period
        self.vol_period = vol_period
        self.j_threshold = j_threshold
    
    def _preprocess(self, ts_code: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算KDJ指标和成交量相关指标
        
        使用基类提供的计算方法：
        1. calculate_kdj() - 计算KDJ指标
        2. 计算成交量相关指标
        
        :param ts_code: 股票代码
        :param df: 日线数据DataFrame
        :return: 包含KDJ和成交量指标的DataFrame
        """
        df_copy = df.copy()
        
        # 确保数据按日期排序
        df_copy = df_copy.sort_values('trade_date').reset_index(drop=True)
        
        # ========== 计算KDJ指标（使用基类方法）==========
        df_copy = self.calculate_kdj(df_copy, period=self.kdj_period)
        
        # ========== 计算成交量指标 ==========
        # 计算前N日的成交量最大值
        df_copy['vol_max_20'] = df_copy['vol'].rolling(window=self.vol_period).max()
        
        # 计算当前成交量与最大值的比例
        df_copy['vol_ratio'] = np.where(
            df_copy['vol_max_20'] > 0,
            df_copy['vol'] / df_copy['vol_max_20'],
            1.0  # 如果最大成交量为0，比例设为1
        )
        
        # 标记是否满足成交量缩量条件（当前成交量 < 最大成交量的1/2）
        df_copy['vol_shrink'] = df_copy['vol_ratio'] < 0.5
        
        # 标记是否满足J值条件（J值 <= 阈值）
        df_copy['kdj_oversold'] = df_copy['kdj_j'] <= self.j_threshold
        
        return df_copy



    
    def _check_stock(self, ts_code: str, df: pd.DataFrame) -> bool:
        """
        判断是否符合买入条件
        
        买入条件：
        1. KDJ的J值 <= 5（超卖信号）
        2. 当前成交量 < 前20日最大成交量的1/2（缩量信号）
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :return: True表示符合买入条件
        """
        # 需要至少vol_period个交易日的数据
        if len(df) < self.vol_period:
            logger.debug(f"{ts_code}: 数据不足，需要至少{self.vol_period}个交易日")
            return False
        
        # 需要至少kdj_period个交易日的数据来计算KDJ
        if len(df) < self.kdj_period:
            logger.debug(f"{ts_code}: 数据不足，无法计算KDJ指标")
            return False
        
        latest = df.iloc[-1]
        
        # 检查J值是否有效
        if pd.isna(latest.get('kdj_j')):
            logger.debug(f"{ts_code}: KDJ的J值无效")
            return False
        
        # 检查成交量数据是否有效
        if pd.isna(latest.get('vol')) or pd.isna(latest.get('vol_max_20')):
            logger.debug(f"{ts_code}: 成交量数据无效")
            return False
        
        # 条件1：KDJ的J值 <= 阈值（超卖信号）
        j_value = latest.get('kdj_j', 999)
        condition1 = j_value <= self.j_threshold
        
        # 条件2：当前成交量 < 前20日最大成交量的1/2（缩量信号）
        vol_ratio = latest.get('vol_ratio', 1.0)
        condition2 = vol_ratio < 0.5
        
        # 两个条件都必须满足
        result = condition1 and condition2
        
        # 添加详细的调试信息
        if result:
            logger.info(
                f"{ts_code}: ✓ 符合少妇战法条件 - "
                f"J值={j_value:.2f} <= {self.j_threshold}, "
                f"成交量比例={vol_ratio:.2%} < 50%"
            )
        else:
            # 输出不满足条件的原因
            reasons = []
            if not condition1:
                reasons.append(f"J值={j_value:.2f} > {self.j_threshold}")
            if not condition2:
                reasons.append(f"成交量比例={vol_ratio:.2%} >= 50%")
            logger.debug(
                f"{ts_code}: ✗ 不符合条件 - " + ", ".join(reasons)
            )
        
        return result
    
    def _explain(self, ts_code: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        生成策略解释信息
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :return: 解释信息字典
        """
        latest = df.iloc[-1]
        
        j_value = float(latest.get('kdj_j', 0))
        k_value = float(latest.get('kdj_k', 0))
        d_value = float(latest.get('kdj_d', 0))
        vol_ratio = float(latest.get('vol_ratio', 1.0))
        vol_max_20 = float(latest.get('vol_max_20', 0))
        current_vol = float(latest.get('vol', 0))
        
        result = {
            "ts_code": ts_code,
            "trade_date": str(latest.get('trade_date', '')),
            "close": float(latest.get('close', 0)),
            "kdj": {
                "K": k_value,
                "D": d_value,
                "J": j_value
            },
            "volume": {
                "current": current_vol,
                "max_20d": vol_max_20,
                "ratio": vol_ratio
            },
            "conditions": {
                "kdj_oversold": bool(latest.get('kdj_oversold', False)),
                "vol_shrink": bool(latest.get('vol_shrink', False))
            },
            "reason": (
                f"KDJ的J值={j_value:.2f} <= {self.j_threshold}（超卖信号），"
                f"成交量={vol_ratio:.2%} < 50%（缩量信号）"
            )
        }
        
        return result

