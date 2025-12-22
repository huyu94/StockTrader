"""
示例策略：简单移动平均策略

这是一个示例策略，展示如何继承 BaseStrategy 并实现自己的策略逻辑。

策略逻辑：
- 计算5日和20日移动平均线
- 当5日均线上穿20日均线时，认为符合买入条件
"""

import pandas as pd
from typing import Dict, Any
from .base_strategy import BaseStrategy
from loguru import logger


class SimpleMAStrategy(BaseStrategy):
    """
    简单移动平均策略
    
    策略说明：
    1. 计算5日移动平均线（MA5）和20日移动平均线（MA20）
    2. 当MA5上穿MA20时，认为符合买入条件
    3. 需要至少20个交易日的数据才能计算MA20
    """
    
    def __init__(self, storage=None, ma_short: int = 5, ma_long: int = 20):
        """
        初始化策略
        
        :param storage: 日线数据存储对象（DailyKlineStorageSQLite实例）
        :param ma_short: 短期移动平均线周期，默认5
        :param ma_long: 长期移动平均线周期，默认20
        """
        super().__init__(storage=storage, name="简单移动平均策略")
        self.ma_short = ma_short
        self.ma_long = ma_long
    
    def _preprocess(self, ts_code: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算移动平均线指标
        
        :param df: 日线数据DataFrame
        :return: 包含MA指标的DataFrame
        """
        df_copy = df.copy()
        
        # 计算移动平均线
        df_copy[f'ma{self.ma_short}'] = df_copy['close'].rolling(window=self.ma_short).mean()
        df_copy[f'ma{self.ma_long}'] = df_copy['close'].rolling(window=self.ma_long).mean()
        
        # 计算均线交叉信号
        # 当前MA5 > MA20 且 前一日MA5 <= MA20 表示上穿
        df_copy['ma_cross'] = (
            (df_copy[f'ma{self.ma_short}'] > df_copy[f'ma{self.ma_long}']) &
            (df_copy[f'ma{self.ma_short}'].shift(1) <= df_copy[f'ma{self.ma_long}'].shift(1))
        )
        
        return df_copy
    
    def _check_stock(self, ts_code: str, df: pd.DataFrame, target_date: str = None) -> bool:
        """
        判断是否符合买入条件
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :return: True表示符合买入条件
        """
        # 需要至少ma_long个交易日的数据
        if len(df) < self.ma_long:
            return False
        
        latest = df.iloc[-1]
        
        # 检查是否有上穿信号
        if latest.get('ma_cross', False):
            return True
        
        # 或者检查当前是否MA5在MA20之上（已形成上升趋势）
        ma_short_col = f'ma{self.ma_short}'
        ma_long_col = f'ma{self.ma_long}'
        
        if ma_short_col in latest and ma_long_col in latest:
            if pd.notna(latest[ma_short_col]) and pd.notna(latest[ma_long_col]):
                return latest[ma_short_col] > latest[ma_long_col]
        
        return False
    
    def _explain(self, ts_code: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        生成策略解释信息
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :return: 解释信息字典
        """
        latest = df.iloc[-1]
        
        ma_short_col = f'ma{self.ma_short}'
        ma_long_col = f'ma{self.ma_long}'
        
        result = {
            "ts_code": ts_code,
            "trade_date": str(latest.get('trade_date', '')),
            "close": float(latest.get('close', 0)),
            "ma_short": float(latest.get(ma_short_col, 0)),
            "ma_long": float(latest.get(ma_long_col, 0)),
            "ma_cross": bool(latest.get('ma_cross', False)),
            "reason": f"MA{self.ma_short}上穿MA{self.ma_long}" if latest.get('ma_cross', False) else f"MA{self.ma_short}在MA{self.ma_long}之上"
        }
        
        return result

