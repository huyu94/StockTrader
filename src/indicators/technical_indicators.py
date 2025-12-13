import pandas as pd
import numpy as np
from scipy.stats import rankdata

class TechnicalIndicators:
    @staticmethod
    def calculate_bbi(df: pd.DataFrame, periods: list = [3, 6, 12, 24]) -> pd.DataFrame:
        """
        计算多空指标BBI
        BBI = (MA3 + MA6 + MA12 + MA24) / 4
        :param df: 包含收盘价的DataFrame，需要有'close'列
        :param periods: 计算BBI的周期列表，默认[3, 6, 12, 24]
        :return: 添加了BBI列的DataFrame
        """
        df_copy = df.copy()
        
        # 计算各周期移动平均线
        for period in periods:
            df_copy[f'MA{period}'] = df_copy['close'].rolling(window=period).mean()
        
        # 计算BBI
        df_copy['BBI'] = df_copy[[f'MA{period}' for period in periods]].mean(axis=1)
        
        # 删除中间MA列
        for period in periods:
            df_copy.drop(columns=[f'MA{period}'], inplace=True)
        
        return df_copy
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> pd.DataFrame:
        """
        计算MACD指标
        :param df: 包含收盘价的DataFrame，需要有'close'列
        :param fast_period: 快速EMA周期，默认12
        :param slow_period: 慢速EMA周期，默认26
        :param signal_period: 信号线周期，默认9
        :return: 添加了MACD、DEA、MACD_HIST列的DataFrame
        """
        df_copy = df.copy()
        
        # 计算EMA
        df_copy['EMA12'] = df_copy['close'].ewm(span=fast_period, adjust=False).mean()
        df_copy['EMA26'] = df_copy['close'].ewm(span=slow_period, adjust=False).mean()
        
        # 计算DIF（MACD线）
        df_copy['MACD'] = df_copy['EMA12'] - df_copy['EMA26']
        
        # 计算DEA（信号线）
        df_copy['DEA'] = df_copy['MACD'].ewm(span=signal_period, adjust=False).mean()
        
        # 计算MACD柱状图
        df_copy['MACD_HIST'] = 2 * (df_copy['MACD'] - df_copy['DEA'])
        
        # 删除中间EMA列
        df_copy.drop(columns=['EMA12', 'EMA26'], inplace=True)
        
        return df_copy
    
    @staticmethod
    def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        计算相对强弱指标RSI
        :param df: 包含收盘价的DataFrame，需要有'close'列
        :param period: 计算周期，默认14
        :return: 添加了RSI列的DataFrame
        """
        df_copy = df.copy()
        
        # 计算价格变动
        delta = df_copy['close'].diff()
        
        # 分离上涨和下跌
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        # 计算RS
        rs = gain / loss
        
        # 计算RSI
        df_copy['RSI'] = 100 - (100 / (1 + rs))
        
        return df_copy
    
    @staticmethod
    def calculate_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """
        计算随机指标KDJ
        :param df: 包含最高价、最低价、收盘价的DataFrame，需要有'high'、'low'、'close'列
        :param n: RSV计算周期，默认9
        :param m1: K值计算周期，默认3
        :param m2: D值计算周期，默认3
        :return: 添加了K、D、J列的DataFrame
        """
        df_copy = df.copy()
        
        # 计算RSV值
        df_copy['RSV'] = 0.0
        for i in range(n-1, len(df_copy)):
            low_min = df_copy['low'].iloc[i-n+1:i+1].min()
            high_max = df_copy['high'].iloc[i-n+1:i+1].max()
            df_copy.loc[df_copy.index[i], 'RSV'] = (df_copy['close'].iloc[i] - low_min) / (high_max - low_min) * 100
        
        # 计算K、D、J值
        df_copy['K'] = 50.0
        df_copy['D'] = 50.0
        
        for i in range(n, len(df_copy)):
            df_copy.loc[df_copy.index[i], 'K'] = (2/3) * df_copy['K'].iloc[i-1] + (1/3) * df_copy['RSV'].iloc[i]
            df_copy.loc[df_copy.index[i], 'D'] = (2/3) * df_copy['D'].iloc[i-1] + (1/3) * df_copy['K'].iloc[i]
        
        # 计算J值
        df_copy['J'] = 3 * df_copy['K'] - 2 * df_copy['D']
        
        # 删除中间RSV列
        df_copy.drop(columns=['RSV'], inplace=True)
        
        return df_copy
    
    @staticmethod
    def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有技术指标
        :param df: 包含OHLC数据的DataFrame
        :return: 添加了所有技术指标列的DataFrame
        """
        df_copy = df.copy()
        df_copy = TechnicalIndicators.calculate_bbi(df_copy)
        df_copy = TechnicalIndicators.calculate_macd(df_copy)
        df_copy = TechnicalIndicators.calculate_rsi(df_copy)
        df_copy = TechnicalIndicators.calculate_kdj(df_copy)
        return df_copy

# 示例用法
if __name__ == "__main__":
    # 这里需要先获取数据，然后调用指标计算函数
    # 例如：
    # from src.data_fetch.stock_data_fetcher import StockDataFetcher
    # fetcher = StockDataFetcher()
    # df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
    # indicators = TechnicalIndicators.calculate_all_indicators(df)
    # print(indicators.tail())
    pass