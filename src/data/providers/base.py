from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional

class BaseProvider(ABC):
    """
    股票数据获取基类
    定义所有数据源必须实现的接口
    """

    @abstractmethod
    def get_stock_basic_info(self) -> pd.DataFrame:
        """
        获取所有股票基本信息
        :return: DataFrame, 包含列: ts_code, symbol, name, area, industry, market, list_date
        """
        pass

    @abstractmethod
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取交易日历
        :param start_date: 开始日期 YYYYMMDD
        :param end_date: 结束日期 YYYYMMDD
        :return: DataFrame, 包含列: exchange, cal_date, is_open
        """
        pass

    @abstractmethod
    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取复权因子
        :param ts_code: 股票代码
        :param start_date: 开始日期 YYYYMMDD
        :param end_date: 结束日期 YYYYMMDD
        :return: DataFrame, 包含列: trade_date, adj_factor
        """
        pass

    @abstractmethod
    def get_daily_k_data(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取单只股票日线数据
        :param ts_code: 股票代码
        :param start_date: 开始日期 YYYYMMDD
        :param end_date: 结束日期 YYYYMMDD
        :return: DataFrame, 包含列: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        """
        pass
