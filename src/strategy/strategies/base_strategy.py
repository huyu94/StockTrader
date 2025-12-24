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
from src.utils.indicator_calculator import IndicatorCalculator


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
        
        # 初始化指标计算器（共享实例）
        self._indicator_calculator = IndicatorCalculator()
        
        if storage is None:
            logger.warning(f"{self.name}: storage not provided, must be set before use")
        else:
            logger.debug(f"Initialized strategy: {self.name} with storage")
    
    @property
    def indicator_calculator(self) -> IndicatorCalculator:
        """获取指标计算器实例"""
        return self._indicator_calculator
    
    def preprocess(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        预处理数据：从storage加载数据并计算技术指标
        
        这是公开方法，子类应该实现 _preprocess 方法。
        此方法会：
        1. 从storage加载股票数据
        2. 验证输入数据
        3. 调用子类的 _preprocess 方法计算指标
        4. 处理异常情况
        
        :param ts_code: 股票代码，例如 "000001.SZ"
        :param start_date: 开始日期（YYYYMMDD格式），如果为None则加载最近一年数据
        :param end_date: 结束日期（YYYYMMDD格式），如果为None则使用今天
        :return: 处理后的DataFrame，包含原始数据和技术指标
        """
        if self.storage is None:
            logger.error(f"{self.name}.preprocess: storage not initialized")
            return pd.DataFrame()
        
        try:
            # 如果未提供日期范围，使用默认值（最近一年）
            if start_date is None or end_date is None:
                from src.utils.date_helper import DateHelper
                if end_date is None:
                    end_date = DateHelper.today()
                if start_date is None:
                    start_date = DateHelper.days_ago(365)
            
            # 从storage加载数据
            df = self.storage.load(ts_code, start_date, end_date)
            
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
            

            # 计算技术指标并拼接
            df_copy = self.indicator_calculator.calculate_all(df_copy)
            

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
    
    def run(self, ts_code: str, start_date: str = None, end_date: str = None, target_date: str = None) -> Dict[str, Any]:
        """
        策略统一入口方法
        
        这是策略的主要入口点，执行完整的策略流程：
        1. 加载并预处理数据
        2. 检查是否符合买入条件
        3. 生成策略解释信息
        
        :param ts_code: 股票代码
        :param start_date: 开始日期（YYYYMMDD格式），如果为None则加载最近一年数据
        :param end_date: 结束日期（YYYYMMDD格式），如果为None则使用今天
        :param target_date: 目标检查日期（YYYYMMDD格式），如果为None则检查最新交易日
        :return: 策略执行结果字典，包含以下字段：
                - success: bool, 是否成功执行
                - ts_code: str, 股票代码
                - target_date: str, 检查的目标日期
                - is_match: bool, 是否符合买入条件
                - signal: str, 交易信号（'买入'/'观望'）
                - data: pd.DataFrame, 预处理后的数据
                - target_row: dict, 目标日期的数据
                - explanation: dict, 策略解释信息
                - error: str, 错误信息（如果有）
        """
        result = {
            "success": False,
            "ts_code": ts_code,
            "target_date": target_date or "最新交易日",
            "signal": "观望",
            # "data": pd.DataFrame(),
            # "target_row": None,
            "explanation": {},
            "error": None
        }
        
        if self.storage is None:
            result["error"] = "storage未初始化"
            logger.error(f"{self.name}.run: storage not initialized")
            return result
        
        try:
            # 步骤1: 预处理数据
            logger.debug(f"{self.name}.run: 预处理数据 {ts_code}")
            processed_df = self.preprocess(ts_code, start_date, end_date)
            
            if processed_df.empty:
                result["error"] = "数据预处理失败或无数据"
                logger.debug(f"Preprocessing returned empty DataFrame for {ts_code}")
                return result
            
            # result["data"] = processed_df
            
            # 获取目标行数据
            if target_date is not None:
                # 转换为字符串格式
                if not pd.api.types.is_datetime64_any_dtype(processed_df['trade_date']):
                    processed_df['trade_date'] = pd.to_datetime(processed_df['trade_date'], errors='coerce')
                
                processed_df['trade_date_str'] = processed_df['trade_date'].dt.strftime('%Y%m%d')
                target_data = processed_df[processed_df['trade_date_str'] == target_date]
                
                if target_data.empty:
                    result["error"] = f"{ts_code} 未找到 {target_date} 的数据，可能不是交易日"
                    logger.warning(f"{ts_code} 未找到 {target_date} 的数据，可能不是交易日")
                    return result
                
                target_row = target_data.iloc[0]
                # result["target_row"] = target_row.to_dict()
            else:
                target_row = processed_df.iloc[-1]
                # result["target_row"] = target_row.to_dict()
                result["target_date"] = str(target_row.get('trade_date', ''))
            
            # 步骤2: 检查是否符合买入条件
            # logger.debug(f"{self.name}.run: 检查买入条件 {ts_code}")
            is_match = self._check_stock(ts_code, processed_df, target_date)
            result["signal"] = "买入" if is_match else "观望"
            
            # 步骤3: 生成解释信息
        # logger.debug(f"{self.name}.run: 生成解释信息 {ts_code}")
            explanation = self._explain(ts_code, processed_df)
            result["explanation"] = explanation
            
            result["success"] = True
            # logger.info(f"{self.name}.run: {ts_code} ({result['target_date']}) - {result['signal']}")
            
            return result
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Error in {self.name}.run for {ts_code}: {e}")
            return result
    
    def check_stock(self, ts_code: str, start_date: str = None, end_date: str = None, target_date: str = None) -> bool:
        """
        判断股票是否符合买入条件
        
        这是公开方法，子类应该实现 _check_stock 方法。
        此方法会：
        1. 预处理数据（调用 preprocess，从storage加载数据并计算指标）
        2. 调用子类的 _check_stock 方法
        3. 处理异常情况
        
        :param ts_code: 股票代码，例如 "000001.SZ"
        :param start_date: 开始日期（YYYYMMDD格式），如果为None则加载最近一年数据
        :param end_date: 结束日期（YYYYMMDD格式），如果为None则使用今天
        :param target_date: 目标检查日期（YYYYMMDD格式），如果为None则检查最新交易日
        :return: True表示符合买入条件，False表示不符合
        """
        if self.storage is None:
            logger.error(f"{self.name}.check_stock: storage not initialized")
            return False
        
        try:
            # 先预处理数据（从storage加载数据并计算指标）
            processed_df = self.preprocess(ts_code, start_date, end_date)
            
            if processed_df.empty:
                logger.debug(f"Preprocessing returned empty DataFrame for {ts_code}")
                return False
            
            # 调用子类的判断方法
            result = self._check_stock(ts_code, processed_df, target_date)
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error in {self.name}.check_stock for {ts_code}: {e}")
            return False
    
    @abstractmethod
    def _check_stock(self, ts_code: str, df: pd.DataFrame, target_date: str = None) -> bool:
        """
        子类必须实现的买入条件判断方法
        
        在此方法中实现具体的买入逻辑，例如：
        - 检查技术指标是否满足条件
        - 检查价格是否在合理区间
        - 检查成交量是否放大
        等
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame（包含技术指标）
        :param target_date: 目标检查日期（YYYYMMDD格式），如果为None则检查最新交易日
        :return: True表示符合买入条件，False表示不符合
        """
        pass
    
    def explain(self, ts_code: str, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        生成策略解释信息
        
        用于结果展示，说明为什么选中这只股票。
        子类可以重写 _explain 方法来自定义解释信息。
        
        :param ts_code: 股票代码
        :param start_date: 开始日期（YYYYMMDD格式），如果为None则加载最近一年数据
        :param end_date: 结束日期（YYYYMMDD格式），如果为None则使用今天
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
            processed_df = self.preprocess(ts_code, start_date, end_date)
            
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
            # "trade_date": latest.get('trade_date', ''),
            "close": float(latest.get('close', 0)),
            "reason": f"符合 {self.name} 策略条件"
        }
    

    
    def __repr__(self) -> str:
        """返回策略的字符串表示"""
        return f"<{self.__class__.__name__}(name='{self.name}')>"

