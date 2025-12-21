"""
策略基类 (BaseStrategy)

所有交易策略的基类，定义了策略的基本接口和通用方法。

策略工作流程：
1. preprocess(ts_code): 从storage加载数据并计算技术指标
2. check_stock(ts_code): 基于处理后的数据，判断股票是否符合买入条件
3. explain(ts_code): 生成策略解释信息，用于结果展示

子类需要实现：
- _preprocess(ts_code, df): 计算指标的具体实现
- _check_stock(ts_code, df): 判断买入条件的具体实现
- _explain(ts_code, df): 生成解释信息的具体实现（可选）
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np
from loguru import logger


class BaseStrategy(ABC):
    """
    策略基类
    
    职责：
    1. 定义策略的标准接口
    2. 提供通用的数据预处理流程（从storage加载数据）
    3. 提供策略执行的标准流程
    
    使用方式：
    ```python
    from src.storage.daily_kline_storage_sqlite import DailyKlineStorageSQLite
    
    storage = DailyKlineStorageSQLite()
    
    class MyStrategy(BaseStrategy):
        def _preprocess(self, ts_code: str, df: pd.DataFrame) -> pd.DataFrame:
            # 计算指标
            df['ma5'] = df['close'].rolling(5).mean()
            return df
        
        def _check_stock(self, ts_code: str, df: pd.DataFrame) -> bool:
            # 判断买入条件
            latest = df.iloc[-1]
            return latest['ma5'] > latest['close']
    
    strategy = MyStrategy(storage=storage)
    if strategy.check_stock("000001.SZ"):
        print("符合买入条件")
    ```
    """
    
    def __init__(self, storage=None, name: str = None):
        """
        初始化策略
        
        :param storage: 日线数据存储对象（DailyKlineStorageSQLite实例）
                       如果不提供，需要在使用前设置 self.storage
        :param name: 策略名称，如果不提供则使用类名
        """
        self.storage = storage
        self.name = name or self.__class__.__name__
        
        if storage is None:
            logger.warning(f"{self.name}: storage not provided, must be set before use")
        else:
            logger.debug(f"Initialized strategy: {self.name} with storage")
    
    def preprocess(self, ts_code: str) -> pd.DataFrame:
        """
        预处理数据：从storage加载数据并计算技术指标
        
        这是公开方法，子类应该实现 _preprocess 方法。
        此方法会：
        1. 从storage加载股票数据
        2. 验证输入数据
        3. 调用子类的 _preprocess 方法计算指标
        4. 处理异常情况
        
        :param ts_code: 股票代码，例如 "000001.SZ"
        :return: 处理后的DataFrame，包含原始数据和技术指标
        """
        if self.storage is None:
            logger.error(f"{self.name}.preprocess: storage not initialized")
            return pd.DataFrame()
        
        try:
            # 从storage加载数据
            df = self.storage.load(ts_code)
            
            if df is None or df.empty:
                logger.debug(f"No data found for {ts_code} in {self.name}.preprocess")
                return pd.DataFrame()
            
            # 验证必要的列
            required_columns = ['trade_date', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns for {ts_code}: {missing_columns}")
                return pd.DataFrame()
            
            # 确保 trade_date 是日期类型
            df_copy = df.copy()
            if 'trade_date' in df_copy.columns:
                if not pd.api.types.is_datetime64_any_dtype(df_copy['trade_date']):
                    df_copy['trade_date'] = pd.to_datetime(df_copy['trade_date'], errors='coerce')
            
            # 按日期排序
            df_copy = df_copy.sort_values('trade_date').reset_index(drop=True)
            
            # 调用子类的预处理方法
            result = self._preprocess(ts_code, df_copy)
            
            if result is None or result.empty:
                logger.warning(f"{self.name}._preprocess returned empty DataFrame for {ts_code}")
                return pd.DataFrame()
            
            return result
            
        except Exception as e:
            logger.error(f"Error in {self.name}.preprocess for {ts_code}: {e}")
            return pd.DataFrame()
    
    @abstractmethod
    def _preprocess(self, ts_code: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        子类必须实现的预处理方法
        
        在此方法中计算技术指标，例如：
        - 移动平均线 (MA)
        - 相对强弱指标 (RSI)
        - KDJ指标
        - MACD指标
        - 布林带 (BOLL)
        等
        
        :param ts_code: 股票代码
        :param df: 已排序的日线数据DataFrame（从storage加载）
        :return: 包含技术指标的DataFrame
        """
        pass
    
    def check_stock(self, ts_code: str) -> bool:
        """
        判断股票是否符合买入条件
        
        这是公开方法，子类应该实现 _check_stock 方法。
        此方法会：
        1. 预处理数据（调用 preprocess，从storage加载数据并计算指标）
        2. 调用子类的 _check_stock 方法
        3. 处理异常情况
        
        :param ts_code: 股票代码，例如 "000001.SZ"
        :return: True表示符合买入条件，False表示不符合
        """
        if self.storage is None:
            logger.error(f"{self.name}.check_stock: storage not initialized")
            return False
        
        try:
            # 先预处理数据（从storage加载数据并计算指标）
            processed_df = self.preprocess(ts_code)
            
            if processed_df.empty:
                logger.debug(f"Preprocessing returned empty DataFrame for {ts_code}")
                return False
            
            # 调用子类的判断方法
            result = self._check_stock(ts_code, processed_df)
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error in {self.name}.check_stock for {ts_code}: {e}")
            return False
    
    @abstractmethod
    def _check_stock(self, ts_code: str, df: pd.DataFrame) -> bool:
        """
        子类必须实现的买入条件判断方法
        
        在此方法中实现具体的买入逻辑，例如：
        - 检查技术指标是否满足条件
        - 检查价格是否在合理区间
        - 检查成交量是否放大
        等
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame（包含技术指标）
        :return: True表示符合买入条件，False表示不符合
        """
        pass
    
    def explain(self, ts_code: str) -> Dict[str, Any]:
        """
        生成策略解释信息
        
        用于结果展示，说明为什么选中这只股票。
        子类可以重写 _explain 方法来自定义解释信息。
        
        :param ts_code: 股票代码
        :return: 包含解释信息的字典
        """
        if self.storage is None:
            logger.error(f"{self.name}.explain: storage not initialized")
            return {
                "ts_code": ts_code,
                "strategy": self.name,
                "reason": "storage未初始化"
            }
        
        try:
            # 预处理数据（从storage加载数据并计算指标）
            processed_df = self.preprocess(ts_code)
            
            if processed_df.empty:
                return {
                    "ts_code": ts_code,
                    "strategy": self.name,
                    "reason": "数据预处理失败"
                }
            
            # 调用子类的解释方法
            result = self._explain(ts_code, processed_df)
            
            # 确保返回字典包含基本信息
            if not isinstance(result, dict):
                result = {}
            
            result.setdefault("ts_code", ts_code)
            result.setdefault("strategy", self.name)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in {self.name}.explain for {ts_code}: {e}")
            return {
                "ts_code": ts_code,
                "strategy": self.name,
                "reason": f"解释生成失败: {str(e)}"
            }
    
    def _explain(self, ts_code: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        子类可选的解释方法
        
        默认实现返回基本信息，子类可以重写以提供更详细的解释。
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :return: 包含解释信息的字典
        """
        latest = df.iloc[-1]
        
        return {
            "ts_code": ts_code,
            "trade_date": latest.get('trade_date', ''),
            "close": float(latest.get('close', 0)),
            "reason": f"符合 {self.name} 策略条件"
        }
    
    # ==================== 技术指标计算方法 ====================
    
    @staticmethod
    def calculate_ma(df: pd.DataFrame, periods: List[int], column: str = 'close') -> pd.DataFrame:
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
        return df_copy
    
    @staticmethod
    def calculate_rsi(df: pd.DataFrame, period: int = 14, column: str = 'close') -> pd.DataFrame:
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
        
        return df_copy
    
    @staticmethod
    def calculate_bbi(df: pd.DataFrame, ma3: int = 3, ma6: int = 6, ma12: int = 12, ma24: int = 24, column: str = 'close') -> pd.DataFrame:
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
        
        return df_copy
    
    @staticmethod
    def calculate_kdj(df: pd.DataFrame, period: int = 9, k_period: int = 3, d_period: int = 3) -> pd.DataFrame:
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
        
        # 计算RSV
        low_min = df_copy['low'].rolling(window=period).min()
        high_max = df_copy['high'].rolling(window=period).max()
        
        rsv = np.where(
            (high_max - low_min) != 0,
            (df_copy['close'] - low_min) / (high_max - low_min) * 100,
            50.0  # 如果最高价等于最低价，RSV设为50
        )
        
        # 计算K值（初始值为50）
        k_values = np.zeros(len(df_copy))
        k_values[0] = 50.0
        
        for i in range(1, len(df_copy)):
            k_values[i] = (2/3) * k_values[i-1] + (1/3) * rsv[i]
        
        df_copy['kdj_k'] = k_values
        
        # 计算D值（初始值为50）
        d_values = np.zeros(len(df_copy))
        d_values[0] = 50.0
        
        for i in range(1, len(df_copy)):
            d_values[i] = (2/3) * d_values[i-1] + (1/3) * k_values[i]
        
        df_copy['kdj_d'] = d_values
        
        # 计算J值
        df_copy['kdj_j'] = 3 * df_copy['kdj_k'] - 2 * df_copy['kdj_d']
        
        return df_copy
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9, column: str = 'close') -> pd.DataFrame:
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
        
        # 计算DIF
        df_copy['macd_dif'] = ema_fast - ema_slow
        
        # 计算DEA（DIF的EMA）
        df_copy['macd_dea'] = df_copy['macd_dif'].ewm(span=signal_period, adjust=False).mean()
        
        # 计算MACD柱状图（Histogram）
        df_copy['macd_hist'] = (df_copy['macd_dif'] - df_copy['macd_dea']) * 2
        
        return df_copy
    
    @staticmethod
    def calculate_boll(df: pd.DataFrame, period: int = 20, num_std: float = 2.0, column: str = 'close') -> pd.DataFrame:
        """
        计算布林带指标 (Bollinger Bands)
        
        中轨 = N日移动平均线
        上轨 = 中轨 + K倍标准差
        下轨 = 中轨 - K倍标准差
        
        :param df: 数据DataFrame
        :param period: 计算周期，默认20
        :param num_std: 标准差倍数，默认2.0
        :param column: 计算布林带的列名，默认 'close'
        :return: 添加了布林带列的DataFrame（boll_mid, boll_upper, boll_lower）
        """
        df_copy = df.copy()
        
        # 中轨（移动平均线）
        df_copy['boll_mid'] = df_copy[column].rolling(window=period).mean()
        
        # 标准差
        std = df_copy[column].rolling(window=period).std()
        
        # 上轨和下轨
        df_copy['boll_upper'] = df_copy['boll_mid'] + (std * num_std)
        df_copy['boll_lower'] = df_copy['boll_mid'] - (std * num_std)
        
        return df_copy
    
    @staticmethod
    def calculate_volume_ratio(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """
        计算成交量比率
        
        当前成交量 / 前N日平均成交量
        
        :param df: 数据DataFrame，必须包含 'vol' 列
        :param period: 比较周期，默认20
        :return: 添加了成交量比率列的DataFrame（vol_ratio）
        """
        df_copy = df.copy()
        
        vol_mean = df_copy['vol'].rolling(window=period).mean()
        df_copy['vol_ratio'] = np.where(
            vol_mean > 0,
            df_copy['vol'] / vol_mean,
            1.0
        )
        
        return df_copy
    
    def __repr__(self) -> str:
        """返回策略的字符串表示"""
        return f"<{self.__class__.__name__}(name='{self.name}')>"

