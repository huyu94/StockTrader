import tushare as ts
import pandas as pd
import os
from datetime import datetime
from src.config import TUSHARE_TOKEN, DATA_PATH

# 初始化Tushare
pro = ts.pro_api(TUSHARE_TOKEN)

class StockDataFetcher:
    def __init__(self):
        self.data_path = DATA_PATH
        # 确保数据目录存在
        os.makedirs(self.data_path, exist_ok=True)
    
    def get_stock_basic_info(self, exchange: str = 'SSE') -> pd.DataFrame:
        """
        获取股票基本信息
        :param exchange: 交易所代码，可选值：SSE（上交所）、SZSE（深交所）、BSE（北交所）
        :return: 股票基本信息DataFrame
        """
        df = pro.stock_basic(exchange=exchange, list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        return df
    
    def get_daily_k_data(self, ts_code: str, start_date: str = None, end_date: str = None, save_local: bool = True) -> pd.DataFrame:
        """
        获取股票日线K线数据
        :param ts_code: 股票代码，如：000001.SZ
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param save_local: 是否保存到本地
        :return: 日线K线数据DataFrame
        """
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values(by='trade_date')
        
        # 保存到本地
        if save_local:
            file_path = os.path.join(self.data_path, f"{ts_code}_daily.csv")
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"数据已保存到：{file_path}")
        
        return df
    
    def get_multi_stocks_daily_k(self, ts_codes: list, start_date: str = None, end_date: str = None) -> dict:
        """
        批量获取多只股票的日线K线数据
        :param ts_codes: 股票代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 股票代码为键，DataFrame为值的字典
        """
        result = {}
        for ts_code in ts_codes:
            try:
                df = self.get_daily_k_data(ts_code, start_date, end_date)
                result[ts_code] = df
                print(f"成功获取{ts_code}的数据")
            except Exception as e:
                print(f"获取{ts_code}的数据失败：{e}")
        return result
    
    def get_index_daily_k_data(self, ts_code: str = '000001.SH', start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取指数日线K线数据
        :param ts_code: 指数代码，默认上证指数
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 指数日线K线数据DataFrame
        """
        df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values(by='trade_date')
        return df
    
    def load_local_data(self, ts_code: str) -> pd.DataFrame:
        """
        从本地加载股票数据
        :param ts_code: 股票代码
        :return: 本地存储的股票数据DataFrame
        """
        file_path = os.path.join(self.data_path, f"{ts_code}_daily.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df
        else:
            print(f"本地数据文件不存在：{file_path}")
            return None

# 示例用法
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 获取股票基本信息
    # basic_info = fetcher.get_stock_basic_info()
    # print(basic_info.head())
    
    # 获取单只股票数据
    # df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
    # print(df.head())
    
    # 批量获取股票数据
    # stocks = ['000001.SZ', '600000.SH', '000858.SZ']
    # result = fetcher.get_multi_stocks_daily_k(stocks, start_date='20240101', end_date='20241231')
    # for ts_code, df in result.items():
    #     print(f"{ts_code} 数据行数：{len(df)}")