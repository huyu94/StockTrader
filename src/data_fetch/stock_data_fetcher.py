import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import TUSHARE_TOKEN, DATA_PATH

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
            
            # 重命名列名
            df = df.rename(columns={
                'trade_date': '日期',
                'adj_factor': '复权因子'
            })
            
            # 选择需要的列
            df = df[['日期', '复权因子']]
            
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
                local_start = local_df['日期'].min().strftime('%Y%m%d')
                local_end = local_df['日期'].max().strftime('%Y%m%d')
                
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
        
        # 重命名列名，符合要求的数据结构
        df = df.rename(columns={
            'trade_date': '日期',
            'open': '开盘价',
            'close': '收盘价',
            'high': '最高价',
            'low': '最低价',
            'vol': '成交量',
            'amount': '成交额'
        })
        
        # 选择需要的列
        df = df[['日期', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额']]
        
        # 保存到本地
        if save_local:
            file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"原始股价数据已保存到：{file_path}")
        
        # 同时获取并保存复权因子
        self.get_adj_factor(ts_code, start_date, end_date, save_local)
        
        return df
    
    def get_batch_daily_k_data(self, ts_code: str = None, trade_date: str = None, start_date: str = None, end_date: str = None, save_local: bool = True) -> dict:
        """
        批量获取股票原始日线K线数据（利用pro_daily接口的批量查询功能）
        :param ts_code: 股票代码，支持多个股票同时提取（逗号分隔），格式：000001.SZ,600000.SH
        :param trade_date: 交易日期，格式：YYYYMMDD
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param save_local: 是否保存到本地
        :return: 股票代码为键，DataFrame为值的字典
        """
        print("=== 开始批量获取股票日线数据 ===")
        
        # 处理参数优先级：trade_date优先于start_date和end_date
        if trade_date:
            # 如果指定了交易日期，忽略start_date和end_date
            start_date = trade_date
            end_date = trade_date
            print(f"交易日期：{trade_date}")
        else:
            # 如果没有指定日期，默认获取最近一年的数据
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if not start_date:
                # 计算一年前的日期
                one_year_ago = datetime.now() - timedelta(days=365)
                start_date = one_year_ago.strftime('%Y%m%d')
            print(f"开始日期：{start_date}")
            print(f"结束日期：{end_date}")
        
        if ts_code:
            print(f"股票代码：{ts_code}")
        
        try:
            # 利用pro_daily接口的批量查询功能
            # 注意：pro_daily接口一次最多返回5000条记录
            # 我们需要分页获取
            all_data = []
            offset = 0
            limit = 5000
            
            while True:
                print(f"正在获取第{offset//limit+1}页数据...")
                # 根据参数调用pro.daily接口
                df = pro.daily(
                    ts_code=ts_code,
                    trade_date=trade_date,
                    start_date=start_date,
                    end_date=end_date,
                    offset=offset,
                    limit=limit
                )
                
                if df.empty:
                    break
                
                all_data.append(df)
                offset += limit
                
                # 如果返回的数据不足limit条，说明已经获取完毕
                if len(df) < limit:
                    break
            
            # 合并所有数据
            if all_data:
                df_all = pd.concat(all_data, ignore_index=True)
                print(f"共获取{len(df_all)}条记录")
            else:
                print("没有获取到数据")
                return {}
            
            # 转换日期格式
            df_all['trade_date'] = pd.to_datetime(df_all['trade_date'], format='%Y%m%d')
            df_all = df_all.sort_values(by=['ts_code', 'trade_date'])
            
            # 重命名列名，符合要求的数据结构
            df_all = df_all.rename(columns={
                'trade_date': '日期',
                'open': '开盘价',
                'close': '收盘价',
                'high': '最高价',
                'low': '最低价',
                'vol': '成交量',
                'amount': '成交额'
            })
            
            # 选择需要的列
            df_all = df_all[['ts_code', '日期', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额']]
            
            # 按股票代码分组，保存到本地
            result = {}
            
            # 获取所有唯一的股票代码
            unique_ts_codes = df_all['ts_code'].unique()
            print(f"共获取{len(unique_ts_codes)}只股票的数据")
            
            # 使用tqdm显示进度
            from tqdm import tqdm
            for ts_code in tqdm(unique_ts_codes, desc="保存股票数据", unit="只"):
                group = df_all[df_all['ts_code'] == ts_code].copy()
                # 移除ts_code列
                group = group.drop(columns=['ts_code'])
                result[ts_code] = group
                
                if save_local:
                    file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
                    group.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            print(f"=== 批量获取所有股票日线数据完成，共获取{len(result)}只股票的数据 ===")
            return result
        except Exception as e:
            print(f"批量获取股票数据失败：{e}")
            return {}
    
    def get_multi_stocks_daily_k(self, ts_codes: list, start_date: str = None, end_date: str = None, max_workers: int = 2, retry_times: int = 3, delay: float = 0.5) -> dict:
        """
        批量获取多只股票的日线K线数据（带重试和限流）
        :param ts_codes: 股票代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param max_workers: 最大并行线程数（Tushare API限制为2）
        :param retry_times: 重试次数
        :param delay: 请求间隔（秒）
        :return: 股票代码为键，DataFrame为值的字典
        """
        result = {}
        import time
        
        def fetch_single_stock(ts_code):
            """获取单只股票数据的内部函数，带重试机制"""
            for attempt in range(retry_times):
                try:
                    # 添加请求间隔
                    time.sleep(delay)
                    df = self.get_daily_k_data(ts_code, start_date, end_date)
                    if df is not None:
                        print(f"成功获取{ts_code}的数据")
                        return ts_code, df
                except Exception as e:
                    print(f"获取{ts_code}的数据失败（第{attempt+1}/{retry_times}次）：{e}")
                    # 如果不是最后一次尝试，增加延迟
                    if attempt < retry_times - 1:
                        time.sleep(delay * 2)
            return ts_code, None
        
        # 使用线程池并行获取数据，最大并行数设为2（Tushare API限制）
        with ThreadPoolExecutor(max_workers=min(max_workers, 2)) as executor:
            # 提交所有任务
            futures = {executor.submit(fetch_single_stock, ts_code): ts_code for ts_code in ts_codes}
            
            # 收集结果
            for future in as_completed(futures):
                ts_code, df = future.result()
                if df is not None:
                    result[ts_code] = df
        
        return result
    
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
    
    def load_local_data(self, ts_code: str) -> pd.DataFrame:
        """
        从本地加载股票数据
        :param ts_code: 股票代码
        :return: 本地存储的股票数据DataFrame
        """
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['日期'] = pd.to_datetime(df['日期'])
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