"""
技术指标计算器 (IndicatorCalculator)

统一管理所有技术指标的计算方法，提供简洁的接口。

支持的指标：
- MA (移动平均线)
- KDJ (随机指标)
- MACD (指数平滑异同移动平均线)
- RSI (相对强弱指标)
- BBI (多空指标)
- BOLL (布林带)

使用示例：
```python
from src.utils.indicator_calculator import IndicatorCalculator

# 初始化计算器
calculator = IndicatorCalculator()

# 计算所有指标
df = calculator.calculate_all(df)

# 或计算单个指标
df = calculator.calculate_ma(df, periods=[5, 10, 20])
df = calculator.calculate_kdj(df)
```
"""

import pandas as pd
import numpy as np
from typing import List, Optional
from loguru import logger


class IndicatorCalculator:
    """
    技术指标计算器
    
    提供各种常用技术指标的计算方法，所有方法都是实例方法，
    返回添加了指标列的新DataFrame（不修改原数据）。
    """
    
    def __init__(self):
        """初始化指标计算器"""
        pass
    
    def calculate_all(
        self, 
        df: pd.DataFrame,
        ma_periods: List[int] = [5, 10, 20,30, 60],
        kdj_period: int = 9,
        macd_fast: int = 12,
        macd_slow: int = 26,
        macd_signal: int = 9,
        rsi_period: int = 14,
        boll_period: int = 20,
        boll_std: float = 2.0
    ) -> pd.DataFrame:
        """
        计算所有技术指标
        
        :param df: 原始K线数据DataFrame
        :param ma_periods: 移动平均线周期列表
        :param kdj_period: KDJ周期
        :param macd_fast: MACD快线周期
        :param macd_slow: MACD慢线周期
        :param macd_signal: MACD信号线周期
        :param rsi_period: RSI周期
        :param boll_period: 布林带周期
        :param boll_std: 布林带标准差倍数
        :return: 包含所有指标的DataFrame
        """
        df_result = df.copy()
        
        # logger.info("开始计算所有技术指标...")
        
        # 数据预检查
        if len(df_result) == 0:
            logger.warning("数据为空，无法计算指标")
            return df_result
        
        # 检查必需列
        required_columns = ['high', 'low', 'close', 'open']
        missing = [col for col in required_columns if col not in df_result.columns]
        if missing:
            logger.error(f"缺少必需的列: {missing}")
            return df_result
        
        # 清理 NaN 值
        nan_count = df_result[required_columns].isna().sum().sum()
        if nan_count > 0:
            logger.warning(f"发现 {nan_count} 个 NaN 值，将被删除")
            df_result = df_result.dropna(subset=required_columns)
        
        # 计算各项指标
        df_result = self.calculate_ma(df_result, periods=ma_periods)
        df_result = self.calculate_kdj(df_result, period=kdj_period)
        df_result = self.calculate_macd(df_result, fast_period=macd_fast, slow_period=macd_slow, signal_period=macd_signal)
        df_result = self.calculate_rsi(df_result, period=rsi_period)
        df_result = self.calculate_bbi(df_result)
        df_result = self.calculate_boll(df_result, period=boll_period, num_std=boll_std)
        
        # logger.info("所有技术指标计算完成")
        
        return df_result
    
    def calculate_ma(self, df: pd.DataFrame, periods: List[int], column: str = 'close') -> pd.DataFrame:
        """
        计算移动平均线 (Moving Average)
        
        :param df: 数据DataFrame
        :param periods: 周期列表，例如 [5, 10, 20, 60]
        :param column: 计算均线的列名，默认 'close'
        :return: 添加了MA列的DataFrame
        """
        df_copy = df.copy()
        
        for period in periods:
            df_copy[f'ma{period}'] = df_copy[column].rolling(window=period).mean()
        
        # logger.debug(f"计算MA: {periods}")
        return df_copy
    
    def calculate_kdj(self, df: pd.DataFrame, period: int = 9, k_period: int = 3, d_period: int = 3) -> pd.DataFrame:
        """
        计算KDJ指标（随机指标）
        
        RSV = (收盘价 - 最低价) / (最高价 - 最低价) * 100
        K值 = (2/3) * 前一日K值 + (1/3) * 当日RSV
        D值 = (2/3) * 前一日D值 + (1/3) * 当日K值
        J值 = 3 * K值 - 2 * D值
        
        :param df: 数据DataFrame，必须包含 'high', 'low', 'close' 列
        :param period: RSV计算周期，默认9
        :param k_period: K值平滑周期，默认3
        :param d_period: D值平滑周期，默认3
        :return: 添加了KDJ列的DataFrame（kdj_k, kdj_d, kdj_j）
        """
        df_copy = df.copy()
        
        # 检查数据是否足够
        if len(df_copy) < period:
            logger.warning(f"数据量不足（{len(df_copy)} < {period}），KDJ将为NaN")
            df_copy['kdj_k'] = np.nan
            df_copy['kdj_d'] = np.nan
            df_copy['kdj_j'] = np.nan
            return df_copy
        
        # 计算RSV
        low_min = df_copy['low'].rolling(window=period).min()
        high_max = df_copy['high'].rolling(window=period).max()
        
        # 处理 NaN 值：用 fillna 填充
        rsv = np.where(
            (high_max - low_min) != 0,
            (df_copy['close'] - low_min) / (high_max - low_min) * 100,
            50.0  # 如果最高价等于最低价，RSV设为50
        )
        
        # 将 NaN 替换为 50（对于 rolling 窗口不足的情况）
        rsv = np.nan_to_num(rsv, nan=50.0)
        
        # 计算K值（初始值为50）
        k_values = np.zeros(len(df_copy))
        k_values[0] = 50.0
        
        for i in range(1, len(df_copy)):
            # 处理可能的 NaN
            if np.isnan(k_values[i-1]) or np.isnan(rsv[i]):
                k_values[i] = 50.0
            else:
                k_values[i] = (2/3) * k_values[i-1] + (1/3) * rsv[i]
        
        df_copy['kdj_k'] = k_values
        
        # 计算D值（初始值为50）
        d_values = np.zeros(len(df_copy))
        d_values[0] = 50.0
        
        for i in range(1, len(df_copy)):
            # 处理可能的 NaN
            if np.isnan(d_values[i-1]) or np.isnan(k_values[i]):
                d_values[i] = 50.0
            else:
                d_values[i] = (2/3) * d_values[i-1] + (1/3) * k_values[i]
        
        df_copy['kdj_d'] = d_values
        
        # 计算J值
        df_copy['kdj_j'] = 3 * df_copy['kdj_k'] - 2 * df_copy['kdj_d']
        
        # logger.debug(f"计算KDJ: period={period}")
        
        return df_copy
    
    def calculate_macd(
        self, 
        df: pd.DataFrame, 
        fast_period: int = 12, 
        slow_period: int = 26, 
        signal_period: int = 9, 
        column: str = 'close'
    ) -> pd.DataFrame:
        """
        计算MACD指标（指数平滑异同移动平均线）
        
        EMA12 = 12日指数移动平均线
        EMA26 = 26日指数移动平均线
        DIF = EMA12 - EMA26
        DEA = DIF的9日指数移动平均线
        MACD = (DIF - DEA) * 2
        
        :param df: 数据DataFrame
        :param fast_period: 快线周期，默认12
        :param slow_period: 慢线周期，默认26
        :param signal_period: 信号线周期，默认9
        :param column: 计算MACD的列名，默认 'close'
        :return: 添加了MACD列的DataFrame（macd_dif, macd_dea, macd_hist）
        """
        df_copy = df.copy()
        
        # 计算EMA
        ema_fast = df_copy[column].ewm(span=fast_period, adjust=False).mean()
        ema_slow = df_copy[column].ewm(span=slow_period, adjust=False).mean()
        
        # 计算DIF (快线 - 慢线)
        df_copy['macd_dif'] = ema_fast - ema_slow
        
        # 计算DEA (DIF的信号线)
        df_copy['macd_dea'] = df_copy['macd_dif'].ewm(span=signal_period, adjust=False).mean()
        
        # 计算MACD柱 (DIF - DEA) * 2
        df_copy['macd_hist'] = (df_copy['macd_dif'] - df_copy['macd_dea']) * 2
        
        # logger.debug(f"计算MACD: fast={fast_period}, slow={slow_period}, signal={signal_period}")
        
        return df_copy
    
    def calculate_rsi(self, df: pd.DataFrame, period: int = 14, column: str = 'close') -> pd.DataFrame:
        """
        计算相对强弱指标 (Relative Strength Index, RSI)
        
        RSI = 100 - (100 / (1 + RS))
        RS = 平均上涨幅度 / 平均下跌幅度
        
        :param df: 数据DataFrame
        :param period: 计算周期，默认14
        :param column: 计算RSI的列名，默认 'close'
        :return: 添加了RSI列的DataFrame
        """
        df_copy = df.copy()
        delta = df_copy[column].diff()
        
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        df_copy[f'rsi{period}'] = 100 - (100 / (1 + rs))
        
        # logger.debug(f"计算RSI: period={period}")
        
        return df_copy
    
    def calculate_bbi(
        self, 
        df: pd.DataFrame, 
        ma3: int = 3, 
        ma6: int = 6, 
        ma12: int = 12, 
        ma24: int = 24, 
        column: str = 'close'
    ) -> pd.DataFrame:
        """
        计算多空指标 (Bull and Bear Index, BBI)
        
        BBI = (MA3 + MA6 + MA12 + MA24) / 4
        
        :param df: 数据DataFrame
        :param ma3: 3日均线周期，默认3
        :param ma6: 6日均线周期，默认6
        :param ma12: 12日均线周期，默认12
        :param ma24: 24日均线周期，默认24
        :param column: 计算BBI的列名，默认 'close'
        :return: 添加了BBI列的DataFrame
        """
        df_copy = df.copy()
        
        ma3_val = df_copy[column].rolling(window=ma3).mean()
        ma6_val = df_copy[column].rolling(window=ma6).mean()
        ma12_val = df_copy[column].rolling(window=ma12).mean()
        ma24_val = df_copy[column].rolling(window=ma24).mean()
        
        df_copy['bbi'] = (ma3_val + ma6_val + ma12_val + ma24_val) / 4
        
        # logger.debug(f"计算BBI: ma3={ma3}, ma6={ma6}, ma12={ma12}, ma24={ma24}")
        
        return df_copy
    
    def calculate_boll(
        self, 
        df: pd.DataFrame, 
        period: int = 20, 
        num_std: float = 2.0, 
        column: str = 'close'
    ) -> pd.DataFrame:
        """
        计算布林带 (Bollinger Bands, BOLL)
        
        中轨 = N日移动平均线
        上轨 = 中轨 + K * N日标准差
        下轨 = 中轨 - K * N日标准差
        
        :param df: 数据DataFrame
        :param period: 计算周期，默认20
        :param num_std: 标准差倍数，默认2.0
        :param column: 计算BOLL的列名，默认 'close'
        :return: 添加了BOLL列的DataFrame（boll_upper, boll_middle, boll_lower）
        """
        df_copy = df.copy()
        
        # 中轨 = 移动平均线
        df_copy['boll_middle'] = df_copy[column].rolling(window=period).mean()
        
        # 标准差
        rolling_std = df_copy[column].rolling(window=period).std()
        
        # 上轨 = 中轨 + K * 标准差
        df_copy['boll_upper'] = df_copy['boll_middle'] + (rolling_std * num_std)
        
        # 下轨 = 中轨 - K * 标准差
        df_copy['boll_lower'] = df_copy['boll_middle'] - (rolling_std * num_std)
        
        # logger.debug(f"计算BOLL: period={period}, std={num_std}")
        
        return df_copy
    
    def calculate_volume_indicators(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """
        计算成交量相关指标
        
        :param df: 数据DataFrame，必须包含 'vol' 列
        :param period: 计算周期，默认20
        :return: 添加了成交量指标的DataFrame
        """
        df_copy = df.copy()
        
        if 'vol' not in df_copy.columns:
            logger.warning("数据中没有 'vol' 列，跳过成交量指标计算")
            return df_copy
        
        # 成交量移动平均
        df_copy['vol_ma'] = df_copy['vol'].rolling(window=period).mean()
        
        # 成交量比率
        df_copy['vol_ratio'] = df_copy['vol'] / df_copy['vol_ma']
        
        # 成交量最大值
        df_copy['vol_max'] = df_copy['vol'].rolling(window=period).max()
        
        # logger.debug(f"计算成交量指标: period={period}")
        
        return df_copy
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        计算平均真实波幅 (Average True Range, ATR)
        
        TR = max(high - low, abs(high - pre_close), abs(low - pre_close))
        ATR = TR的N日移动平均
        
        :param df: 数据DataFrame，必须包含 'high', 'low', 'close' 列
        :param period: 计算周期，默认14
        :return: 添加了ATR列的DataFrame
        """
        df_copy = df.copy()
        
        # 计算前一日收盘价
        df_copy['pre_close_shift'] = df_copy['close'].shift(1)
        
        # 计算真实波幅
        df_copy['tr'] = df_copy[['high', 'low', 'pre_close_shift']].apply(
            lambda row: max(
                row['high'] - row['low'],
                abs(row['high'] - row['pre_close_shift']) if pd.notna(row['pre_close_shift']) else 0,
                abs(row['low'] - row['pre_close_shift']) if pd.notna(row['pre_close_shift']) else 0
            ),
            axis=1
        )
        
        # 计算ATR
        df_copy['atr'] = df_copy['tr'].rolling(window=period).mean()
        
        # 删除临时列
        df_copy = df_copy.drop(['pre_close_shift', 'tr'], axis=1)
        
        # logger.debug(f"计算ATR: period={period}")
        
        return df_copy

