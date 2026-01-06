"""
策略基类模块

定义所有策略的抽象基类
"""

from abc import ABC, abstractmethod
from typing import List, Union, Optional
import pandas as pd
from loguru import logger


class BaseStrategy(ABC):
    """
    策略抽象基类
    
    所有策略都应该继承此类并实现以下方法：
    1. calculate_indicators: 计算技术指标
    2. filter_stocks: 根据策略规则筛选股票
    """
    
    def __init__(self, name: Optional[str] = None):
        """
        初始化策略
        
        Args:
            name: 策略名称，如果不提供则使用类名
        """
        self.name = name or self.__class__.__name__
        logger.debug(f"初始化策略: {self.name}")
    
    @abstractmethod
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标
        
        在DataFrame中添加策略所需的技术指标列
        
        Args:
            df: 股票K线数据DataFrame，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - open, high, low, close: 价格数据（前复权）
                - vol: 成交量
                - amount: 成交额
                可能还包含其他列（如历史数据的前复权价格字段）
        
        Returns:
            pd.DataFrame: 添加了技术指标列后的DataFrame
        """
        pass
    
    @abstractmethod
    def filter_stocks(self, df: pd.DataFrame) -> Union[List[str], pd.DataFrame]:
        """
        根据策略规则筛选股票
        
        Args:
            df: 包含技术指标的K线数据DataFrame
        
        Returns:
            Union[List[str], pd.DataFrame]: 
                - 如果返回List[str]，则为符合条件的股票代码列表
                - 如果返回pd.DataFrame，则为包含筛选结果的DataFrame（至少包含ts_code列）
        """
        pass
    
    def run(self, df: pd.DataFrame) -> Union[List[str], pd.DataFrame]:
        """
        运行策略：计算指标并筛选股票
        
        Args:
            df: 股票K线数据DataFrame
        
        Returns:
            Union[List[str], pd.DataFrame]: 筛选结果
        """
        # 计算指标
        df_with_indicators = self.calculate_indicators(df)
        
        # 筛选股票
        result = self.filter_stocks(df_with_indicators)
        
        return result

