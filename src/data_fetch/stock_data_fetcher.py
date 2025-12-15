import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta
from src.config import TUSHARE_TOKEN, DATA_PATH

# 初始化Tushare
pro = ts.pro_api(TUSHARE_TOKEN)

class StockDataFetcher:
    def __init__(self):
        self.data_path = os.path.join(DATA_PATH, "stock_data")
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
    
    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票复权因子
        :param ts_code: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 复权因子DataFrame
        """
        try:
            df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.sort_values(by='trade_date')
            return df
        except Exception as e:
            print(f"获取{ts_code}的复权因子失败：{e}")
            return None
    
    def get_daily_k_data(self, ts_code: str, start_date: str = None, end_date: str = None, 
                        adj_type: str = 'qfq', save_local: bool = True) -> pd.DataFrame:
        """
        获取股票日线K线数据，支持前复权
        :param ts_code: 股票代码，如：000001.SZ
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param adj_type: 复权类型，qfq=前复权，hfq=后复权，None=不复权
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
        
        # 处理复权
        if adj_type == 'qfq':
            # 获取复权因子
            adj_factor_df = self.get_adj_factor(ts_code, start_date, end_date)
            if adj_factor_df is not None and not adj_factor_df.empty:
                # 合并复权因子
                df = pd.merge(df, adj_factor_df[['trade_date', 'adj_factor']], on='trade_date', how='left')
                
                # 计算前复权价格
                for price_col in ['open', 'high', 'low', 'close']:
                    df[price_col] = df[price_col] * df['adj_factor'] / df['adj_factor'].iloc[-1]
                
                # 删除复权因子列
                df = df.drop(columns=['adj_factor'])
        
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
            file_path = os.path.join(self.data_path, f"{ts_code}.csv")
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"数据已保存到：{file_path}")
        
        return df
    
    def get_multi_stocks_daily_k(self, ts_codes: list, start_date: str = None, end_date: str = None,
                                 adj_type: str = 'qfq') -> dict:
        """
        批量获取多只股票的日线K线数据
        :param ts_codes: 股票代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param adj_type: 复权类型
        :return: 股票代码为键，DataFrame为值的字典
        """
        result = {}
        for ts_code in ts_codes:
            try:
                df = self.get_daily_k_data(ts_code, start_date, end_date, adj_type)
                if df is not None:
                    result[ts_code] = df
                    print(f"成功获取{ts_code}的数据")
            except Exception as e:
                print(f"获取{ts_code}的数据失败：{e}")
        return result
    
    def fetch_all_stocks_last_year(self, adj_type: str = 'qfq') -> None:
        """
        爬取最近一年所有股票数据
        :param adj_type: 复权类型
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
                self.get_multi_stocks_daily_k(batch_codes, adj_type=adj_type)
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
        file_path = os.path.join(self.data_path, f"{ts_code}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df['日期'] = pd.to_datetime(df['日期'])
            return df
        else:
            print(f"本地数据文件不存在：{file_path}")
            return None
    
    def update_stock_data(self, ts_code: str, adj_type: str = 'qfq') -> None:
        """
        更新股票数据，获取最新数据
        :param ts_code: 股票代码
        :param adj_type: 复权类型
        """
        print(f"=== 更新{ts_code}数据 ===")
        
        # 检查本地是否已有数据
        local_df = self.load_local_data(ts_code)
        
        if local_df is not None:
            # 获取本地数据的最新日期
            last_date = local_df['日期'].max().strftime('%Y%m%d')
            # 获取从最新日期到现在的数据
            new_df = self.get_daily_k_data(ts_code, start_date=last_date, adj_type=adj_type, save_local=False)
            
            if new_df is not None and not new_df.empty:
                # 合并数据，去重
                combined_df = pd.concat([local_df, new_df])
                combined_df = combined_df.drop_duplicates(subset=['日期'])
                combined_df = combined_df.sort_values(by='日期')
                
                # 保存更新后的数据
                file_path = os.path.join(self.data_path, f"{ts_code}.csv")
                combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"{ts_code}数据已更新，共{len(combined_df)}条记录")
            else:
                print(f"{ts_code}没有新数据需要更新")
        else:
            # 本地没有数据，直接获取最近一年的数据
            self.get_daily_k_data(ts_code, adj_type=adj_type, save_local=True)
    
    def update_all_stocks_data(self, adj_type: str = 'qfq') -> None:
        """
        更新所有股票数据
        :param adj_type: 复权类型
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
                self.update_stock_data(ts_code, adj_type=adj_type)
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