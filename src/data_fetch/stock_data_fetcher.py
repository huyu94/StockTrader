import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import TUSHARE_TOKEN, DATA_PATH
from src.data_fetch.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS

# 初始化Tushare
pro = ts.pro_api(TUSHARE_TOKEN)

class StockDataFetcher:
    def __init__(self):
        self.stock_data_path = os.path.join(DATA_PATH, "stock_data")
        self.adj_factor_path = os.path.join(DATA_PATH, "adj_factor")
        # 确保数据目录存在
        os.makedirs(self.stock_data_path, exist_ok=True)
        os.makedirs(self.adj_factor_path, exist_ok=True)
    
    
    def get_stock_basic_info(self, exchange: str = 'SSE') -> pd.DataFrame:
        """
        获取股票基本信息
        :param exchange: 交易所代码，可选值：SSE（上交所）、SZSE（深交所）、BSE（北交所）
        :return: 股票基本信息DataFrame
        """
        df = pro.stock_basic(exchange=exchange, list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        return df
    
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
            self.save_daily_k_data(ts_code, df)
        
        # 同时获取并保存复权因子
        self.get_adj_factor(ts_code, start_date, end_date, save_local)
        
        return df
    

    def save_daily_k_data(self, ts_code: str, df: pd.DataFrame) -> None:








    
    
    def fetch_all_stocks_last_year(self) -> None:
        """
        爬取最近一年所有股票数据
        """
        print("=== 开始爬取最近一年所有股票数据 ===")
        
        # 获取所有股票代码
        stock_codes = self.get_all_stock_codes()
        print(f"共{len(stock_codes)}只股票需要爬取")
        
        # 批量获取数据，每批100只股票
        batch_size = 100
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i+batch_size]
            print(f"\n正在爬取第{i//batch_size+1}批，共{len(batch_codes)}只股票")
            
            try:
                self.get_multi_stocks_daily_k(batch_codes)
            except Exception as e:
                print(f"爬取第{i//batch_size+1}批失败：{e}")
        
        print("\n=== 所有股票数据爬取完成 ===")
    
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
    
    def detect_missing_dates(self, ts_code: str = None, start_date: str = None, end_date: str = None, df: pd.DataFrame = None) -> pd.DatetimeIndex:
        """
        检测股票数据中缺失的交易日
        :param ts_code: 股票代码，若提供则从本地加载数据
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
        
        # 转换为datetime格式
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        print(f"检测区间：{start_date} 至 {end_date}")
        
        # 获取数据
        if df is None:
            if ts_code:
                df = self.load_local_data(ts_code)
                if df is None:
                    print(f"未找到{ts_code}的本地数据")
                    return pd.DatetimeIndex([])
            else:
                print("必须提供ts_code或df参数")
                return pd.DatetimeIndex([])
        
        # 确保日期列是datetime格式
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            existing_dates = df['trade_date']
        elif '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'])
            existing_dates = df['日期']
        else:
            print("数据中没有日期列")
            return pd.DatetimeIndex([])
        
        # 1. 获取完整的交易日序列
        full_trade_dates = []
        
        # 遍历年份，获取每个年份的交易日历
        for year in range(start_dt.year, end_dt.year + 1):
            # 尝试从本地加载交易日历
            calendar_path = os.path.join(DATA_PATH, f"trade_calendar_{year}.csv")
            
            if os.path.exists(calendar_path):
                # 从本地加载交易日历
                calendar_df = pd.read_csv(calendar_path)
                
                # 转换cal_date为datetime格式（注意文件中的格式是YYYYMMDD字符串）
                calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
                
                # 筛选出交易日（is_open=1）
                year_trade_dates = calendar_df[calendar_df['is_open'] == 1]['cal_date']
                
                # 筛选出指定日期范围内的交易日
                year_trade_dates = year_trade_dates[(year_trade_dates >= start_dt) & (year_trade_dates <= end_dt)]
                
                # 添加到完整序列中
                full_trade_dates.extend(year_trade_dates.tolist())
            else:
                # 如果本地文件不存在，从API获取
                print(f"正在获取{year}年交易日历...")
                start_date_year = f"{year}0101"
                end_date_year = f"{year}1231"
                calendar_df = pro.trade_cal(exchange='', start_date=start_date_year, end_date=end_date_year, fields='cal_date,is_open')
                
                # 转换cal_date为datetime格式
                calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
                
                # 筛选出交易日（is_open=1）和指定日期范围
                year_trade_dates = calendar_df[(calendar_df['is_open'] == 1) & 
                                             (calendar_df['cal_date'] >= start_dt) & 
                                             (calendar_df['cal_date'] <= end_dt)]['cal_date']
                
                # 添加到完整序列中
                full_trade_dates.extend(year_trade_dates.tolist())
                
                # 保存到本地
                calendar_df.to_csv(calendar_path, index=False, encoding='utf-8-sig')
                print(f"{year}年交易日历已保存到：{calendar_path}")
        
        # 转换为datetime index并去重排序
        full_trade_dates = pd.DatetimeIndex(full_trade_dates)
        full_trade_dates = full_trade_dates.unique().sort_values()
        
        # 转换现有日期为date格式，以便对比
        existing_dates_date = existing_dates.dt.date
        
        # DatetimeIndex直接使用.date属性，不需要dt访问器
        full_trade_dates_date = full_trade_dates.date
        
        # 4. 对比找出缺失的交易日
        missing_dates = full_trade_dates[~pd.Series(full_trade_dates_date).isin(existing_dates_date)]
        
        # 5. 输出结果
        print(f"总应爬交易日数：{len(full_trade_dates)}")
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

    def load_local_data(self, ts_code: str) -> pd.DataFrame:
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
    
    def update_stock_data(self, ts_code: str) -> None:
        """
        更新股票数据，获取最新数据
        :param ts_code: 股票代码
        """
        print(f"=== 更新{ts_code}数据 ===")
        
        # 检查本地是否已有数据
        local_df = self.load_local_data(ts_code)
        
        if local_df is not None:
            # 获取本地数据的最新日期
            last_date = local_df['日期'].max().strftime('%Y%m%d')
            # 获取从最新日期到现在的数据
            new_df = self.get_daily_k_data(ts_code, start_date=last_date, save_local=False)
            
            if new_df is not None and not new_df.empty:
                # 合并数据，去重
                combined_df = pd.concat([local_df, new_df])
                combined_df = combined_df.drop_duplicates(subset=['日期'])
                combined_df = combined_df.sort_values(by='日期')
                
                # 保存更新后的数据
                file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
                combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"{ts_code}数据已更新，共{len(combined_df)}条记录")
            else:
                print(f"{ts_code}没有新数据需要更新")
        else:
            # 本地没有数据，直接获取最近一年的数据
            self.get_daily_k_data(ts_code, save_local=True)
    
    def update_all_stocks_data(self) -> None:
        """
        更新所有股票数据
        """
        print("=== 开始更新所有股票数据 ===")
        
        # 获取所有股票代码
        stock_codes = self.get_all_stock_codes()
        print(f"共{len(stock_codes)}只股票需要更新")
        
        # 逐个更新股票数据
        for i, ts_code in enumerate(stock_codes):
            if i % 50 == 0:
                print(f"\n已更新{i}只股票")
            
            try:
                self.update_stock_data(ts_code)
            except Exception as e:
                print(f"更新{ts_code}失败：{e}")
        
        print("\n=== 所有股票数据更新完成 ===")

# 示例用法
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 获取单只股票数据
    # df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
    
    # 爬取最近一年所有股票数据
    # fetcher.fetch_all_stocks_last_year()
    
    # 更新单只股票数据
    # fetcher.update_stock_data('000001.SZ')
    
    # 更新所有股票数据
    # fetcher.update_all_stocks_data()