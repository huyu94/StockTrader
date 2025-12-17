import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Union

from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import TUSHARE_TOKEN, DATA_PATH
from src.data_fetch.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS

# 初始化Tushare
pro = ts.pro_api(TUSHARE_TOKEN)

class StockDailyKLineFetcher:

    def __init__(self):
        self.stock_data_path = os.path.join(DATA_PATH, "stock_data")
        self.adj_factor_path = os.path.join(DATA_PATH, "adj_factor")
        self.stock_basic_info_path = os.path.join(DATA_PATH, "stock_basic_info.csv")
        self.trade_calendar_path = os.path.join(DATA_PATH, "trade_calendar.csv")

        # 确保数据目录存在
        os.makedirs(self.stock_data_path, exist_ok=True)
        os.makedirs(self.adj_factor_path, exist_ok=True)


    
    def get_stock_basic_info(self, exchange: str = 'SSE', save_local: bool = True) -> pd.DataFrame:
        """
        获取股票基本信息
        :param exchange: 交易所代码，可选值：SSE（上交所）、SZSE（深交所）、BSE（北交所）
        :return: 股票基本信息DataFrame
        """
        df = pro.stock_basic(exchange=exchange, list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        if save_local:
            self.save_stock_basic_info(df)
        return df

    def save_stock_basic_info(self, df: pd.DataFrame):
        """
        保存股票基本信息到本地
        :param df: 股票基本信息DataFrame
        """
        origin_df = pd.read_csv(self.stock_basic_info_path)
        # 合并数据，保留所有唯一的股票代码
        merged_df = pd.concat([origin_df, df]).drop_duplicates(subset=['ts_code'], keep='last')
        merged_df.to_csv(self.stock_basic_info_path, index=False, encoding='utf-8-sig')
        print(f"股票基本信息已保存到：{self.stock_basic_info_path}")

    def get_all_stock_codes(self) -> list:
        """
        获取所有A股股票代码
        :return: 股票代码列表
        """
        # 获取所有交易所的股票
        stock_basic = pd.concat([
            self.get_stock_basic_info('SSE'),
            self.get_stock_basic_info('SZSE'),
            self.get_stock_basic_info('BSE')
        ])
        return stock_basic['ts_code'].tolist()

    def get_trade_calendar(self, exchange: str = 'SSE', start_date: str = '20000101', end_date: str = '20500101') -> pd.DataFrame:
        """
        获取交易日历
        :return: 交易日历DataFrame
        """
        df = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date)
        df.to_csv(self.trade_calendar_path, index=False, encoding='utf-8-sig')
        print(f"交易日历已保存到：{self.trade_calendar_path}")
        return df


    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str, save_local: bool = True) -> pd.DataFrame:
        """
        获取股票复权因子
        :param ts_code: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param save_local: 是否保存到本地
        :return: 复权因子DataFrame
        """
        try:
            df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df.empty:
                print(f"{ts_code} 没有获取到复权因子数据")
                return None
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.sort_values(by='trade_date')
            

            
            # 选择需要的列
            df = df[ADJ_FACTOR_COLUMN_MAPPINGS.keys()]
            
            # 保存到本地
            if save_local:
                file_path = os.path.join(self.adj_factor_path, f"{ts_code}.csv")
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"复权因子数据已保存到：{file_path}")
            
            return df
        except Exception as e:
            print(f"获取{ts_code}的复权因子失败：{e}")
            return None
    
    def get_daily_k_data(self, ts_code: str, start_date: str = None, end_date: str = None, 
                        save_local: bool = True) -> pd.DataFrame:
        """
        获取单只股票原始日线K线数据
        :param ts_code: 股票代码，如：000001.SZ
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param save_local: 是否保存到本地
        :return: 日线K线数据DataFrame
        """
        # 检查本地是否已有数据
        local_df = self.load_local_data(ts_code)
        if local_df is not None:
            # 检查本地数据是否覆盖了请求的日期范围
            if start_date is None and end_date is None:
                # 如果没有指定日期范围，直接返回本地数据
                print(f"{ts_code}：使用本地已有数据，共{len(local_df)}行")
                return local_df
            else:
                # 如果指定了日期范围，检查本地数据是否覆盖该范围
                local_start = local_df['trade_date'].min().strftime('%Y%m%d')
                local_end = local_df['trade_date'].max().strftime('%Y%m%d')
                
                # 检查请求的日期范围是否完全在本地数据范围内
                if (start_date is None or start_date <= local_start) and (end_date is None or end_date >= local_end):
                    print(f"{ts_code}：使用本地已有数据，共{len(local_df)}行")
                    return local_df
        
        # 如果没有本地数据或本地数据不覆盖请求的日期范围，则从API获取
        print(f"{ts_code}：本地数据不存在或不完整，从API获取")
        
        # 如果没有指定日期，默认获取最近一年的数据
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            # 计算一年前的日期
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        
        # 获取日线数据
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df.empty:
            print(f"{ts_code} 没有获取到数据")
            return None
        
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values(by='trade_date')
        
        
        # 选择需要的列
        df = df[DAILY_COLUMN_MAPPINGS.keys()]
        
        # 保存到本地
        if save_local:
            self.save_to_local_data(ts_code, df)
        
        # 同时获取并保存复权因子
        self.get_adj_factor(ts_code, start_date, end_date, save_local)
        
        return df

    def detect_missing_dates(self, exchange: str = 'SSE', start_date: str = None, end_date: str = None, df: pd.DataFrame = None) -> pd.DatetimeIndex:
        """
        检测股票数据中缺失的交易日
        :param exchange: 交易所，默认SSE
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param df: 若提供则直接使用，否则从本地加载
        :return: 缺失的交易日索引
        """
        print("=== 开始检测缺失交易日 ===")

        # 处理日期参数
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            # 计算一年前的日期
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        
        print(f"检测区间：{start_date} 至 {end_date}")
        

        # 1. 获取完整的交易日序列
        calendar_df = self.get_trade_calendar(exchange=exchange, start_date=start_date, end_date=end_date)
        calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
        trade_df = calendar_df[calendar_df['is_open'] == 1]
        trade_dates = pd.DatetimeIndex(trade_df['cal_date'].tolist())

        # 2. 获取df中存在的交易日
        if df is None or df.empty:
            existing_dates = []
        else:
            existing_dates = pd.DatetimeIndex(df['trade_date'].tolist())

        # 3. 对比找出缺失的交易日
        missing_dates = trade_dates.difference(existing_dates)
        missing_dates = missing_dates.sort_values()
        
        # 4. 输出结果
        print(f"总应爬交易日数：{len(trade_dates)}")
        print(f"已爬交易日数：{len(existing_dates)}")
        print(f"缺失交易日数：{len(missing_dates)}")
        
        if len(missing_dates) > 0:
            print(f"缺失日期列表：\n{missing_dates.strftime('%Y-%m-%d').tolist()}")
        
        return missing_dates
    
    def fill_missing_data(self, ts_code: str, start_date: str = None, end_date: str = None) -> None:
        """
        自动补爬股票数据中缺失的交易日
        :param ts_code: 股票代码
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        """
        print(f"=== 开始补爬{ts_code}缺失数据 ===")
        
        # 检测缺失日期
        missing_dates = self.detect_missing_dates(ts_code, start_date, end_date)
        
        if len(missing_dates) == 0:
            print(f"{ts_code} 没有缺失数据，无需补爬")
            return
        
        # 转换缺失日期为字符串格式，用于API调用
        missing_dates_str = missing_dates.strftime('%Y%m%d').tolist()
        
        # 按时间顺序排序
        missing_dates_str.sort()
        
        # 分组获取缺失数据，每次获取连续的日期范围
        # 这里简化处理，直接使用第一个和最后一个日期作为范围
        start_missing = missing_dates_str[0]
        end_missing = missing_dates_str[-1]
        
        print(f"开始补爬{start_missing}至{end_missing}的缺失数据...")
        
        # 调用get_daily_k_data方法获取缺失数据
        missing_df = self.get_daily_k_data(ts_code, start_missing, end_missing, save_local=False)
        
        if missing_df is not None and not missing_df.empty:
            print(f"成功获取{len(missing_df)}条缺失数据")
            
            # 加载现有数据
            existing_df = self.load_local_data(ts_code)
            
            # 合并数据
            combined_df = pd.concat([existing_df, missing_df])
            
            # 去重
            combined_df = combined_df.drop_duplicates(subset=['trade_date'])
            
            # 按日期排序
            combined_df = combined_df.sort_values(by=['trade_date'])
            
            # 保存合并后的数据
            file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
            combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"已将补爬数据合并到本地文件：{file_path}")
            print(f"合并后数据共{len(combined_df)}条")
        else:
            print("未获取到缺失数据")
        
        print(f"=== {ts_code}缺失数据补爬完成 ===")

    def save_to_local_data(self, ts_code: str, df: pd.DataFrame) -> None:
        """
        保存原始日线K线数据到本地
        :param ts_code: 股票代码
        :param df: 原始日线K线数据DataFrame
        """
        # 检查传入数据是否为空
        if df is None or df.empty:
            print(f"{ts_code}：传入的数据为空，跳过保存")
            return
        # 生成文件路径
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        # 读取本地文件
        origin_df = self.load_local_data(ts_code)
        # 合并数据
        if origin_df is None or origin_df.empty:
            merged_df = df.copy()
        else:
            merged_df = pd.concat([origin_df, df], ignore_index=True)
        # 去重
        merged_df = merged_df.drop_duplicates(subset=['trade_date'], keep='last')
        # 按日期排序
        merged_df = merged_df.sort_values(by='trade_date')
        # 存储
        merged_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"原始股价数据已保存到：{file_path}")

    def load_local_data(self, ts_code: str) -> Union[pd.DataFrame, None]:

        """
        从本地加载股票数据
        :param ts_code: 股票代码
        :return: 本地存储的股票数据DataFrame
        """
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            # 转换日期列为datetime格式
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df
        else:
            print(f"本地数据文件不存在：{file_path}")
            return None



# 示例用法
if __name__ == "__main__":
    fetcher = StockDailyKLineFetcher()

    
    # 获取单只股票数据
    # df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
    
    # 爬取最近一年所有股票数据
    # fetcher.fetch_all_stocks_last_year()
    
    # 更新单只股票数据
    # fetcher.update_stock_data('000001.SZ')
    
    # 更新所有股票数据
    # fetcher.update_all_stocks_data()