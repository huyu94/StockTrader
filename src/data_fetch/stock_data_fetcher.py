from loguru import logger
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from functools import cached_property

from project_var import DATA_DIR, PROJECT_DIR
from src.data_fetch.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS
from src.data_fetch.providers.base import BaseProvider
from src.data_fetch.providers.tushare_provider import TushareProvider
from src.data_fetch.providers.akshare_provider import AkShareProvider

class StockDailyKLineFetcher:

    def __init__(self, provider_name: str = "tushare"):
        self.stock_data_path = os.path.join(DATA_DIR, "stock_data")
        self.adj_factor_path = os.path.join(DATA_DIR, "adj_factor")
        self.stock_basic_info_path = os.path.join(DATA_DIR, "stock_basic_info.csv")
        self.trade_calendar_path = os.path.join(DATA_DIR, "trade_calendar.csv")
        self.calendar_cache_dir = os.path.join(PROJECT_DIR, "cache")
        os.makedirs(self.calendar_cache_dir, exist_ok=True)

        # 确保数据目录存在
        os.makedirs(self.stock_data_path, exist_ok=True)
        os.makedirs(self.adj_factor_path, exist_ok=True)
        
        if provider_name == "akshare":
            self.provider: BaseProvider = AkShareProvider()
        else:
            self.provider: BaseProvider = TushareProvider()

    def get_stock_basic_info(self, exchange: str = 'SSE', save_local: bool = True) -> pd.DataFrame:
        """
        获取股票基本信息
        :param exchange: 交易所代码，可选值：SSE（上交所）、SZSE（深交所）、BSE（北交所）
        :return: 股票基本信息DataFrame
        """
        df = self.provider.get_stock_basic_info()
        logger.info(f"获取股票基本信息完成，共{len(df)}条记录")
        
        # 简单过滤
        if exchange and 'exchange' in df.columns:
             # 注意：如果是空字符串或None，则不过滤
             filtered_df = df[df['exchange'] == exchange]
             if not filtered_df.empty:
                 df = filtered_df

        if save_local:
            self.save_stock_basic_info(df)
        return df

    def save_stock_basic_info(self, df: pd.DataFrame):
        """
        保存股票基本信息到本地
        :param df: 股票基本信息DataFrame
        """        
        # 检查文件是否存在
        if os.path.exists(self.stock_basic_info_path):
            origin_df = pd.read_csv(self.stock_basic_info_path)
            # 合并数据，保留所有唯一的股票代码
            merged_df = pd.concat([origin_df, df]).drop_duplicates(subset=['ts_code'], keep='last')
        else:
            # 如果文件不存在，直接保存当前数据
            merged_df = df.copy()
            
        merged_df.to_csv(self.stock_basic_info_path, index=False, encoding='utf-8-sig')
        logger.info(f"股票基本信息已保存到：{self.stock_basic_info_path}")

    def get_all_stock_codes(self) -> list:
        """
        获取所有A股股票代码
        :return: 股票代码列表
        """
        # 优先从本地读取
        if os.path.exists(self.stock_basic_info_path):
            df = pd.read_csv(self.stock_basic_info_path)
            return df['ts_code'].tolist()
        
        # 获取所有交易所的股票
        # 这里原来的逻辑是拼接三个交易所，现在我们调用 get_stock_basic_info 
        # 如果 provider 返回了所有，那就直接返回
        df = self.get_stock_basic_info(exchange='')
        return df['ts_code'].tolist()

    def _calendar_cache_path(self, exchange: str = 'SSE') -> str:
        return os.path.join(self.calendar_cache_dir, f"trade_calendar_{exchange}.csv")

    @cached_property
    def trade_calendar_df(self) -> pd.DataFrame:
        path = self._calendar_cache_path('SSE')
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, encoding='utf-8-sig')
                return df
            except Exception:
                pass
        start_date = '20000101'
        end_date = datetime.now().strftime('%Y%m%d')
        df = self.provider.get_trade_calendar(start_date=start_date, end_date=end_date)
        df = df.drop_duplicates().sort_values(by='cal_date')
        tmp = path + ".tmp"
        df.to_csv(tmp, index=False, encoding='utf-8-sig')
        os.replace(tmp, path)
        return df

    def ensure_calendar_coverage(self, start_date: str, end_date: str, exchange: str = 'SSE') -> None:
        df = self.trade_calendar_df
        cached_min = df['cal_date'].min() if 'cal_date' in df.columns and not df.empty else None
        cached_max = df['cal_date'].max() if 'cal_date' in df.columns and not df.empty else None
        need_fetch = False
        fetch_ranges = []
        if cached_min is None or start_date < str(cached_min):
            need_fetch = True
            fetch_ranges.append((start_date, str(cached_min) if cached_min else end_date))
        if cached_max is None or end_date > str(cached_max):
            need_fetch = True
            fetch_ranges.append((str(cached_max) if cached_max else start_date, end_date))
        if need_fetch:
            parts = []
            for s, e in fetch_ranges:
                if s and e and s <= e:
                    parts.append(self.provider.get_trade_calendar(start_date=s, end_date=e))
            if parts:
                add_df = pd.concat(parts, ignore_index=True)
                merged = pd.concat([df, add_df], ignore_index=True)
                merged = merged.drop_duplicates().sort_values(by='cal_date')
                path = self._calendar_cache_path(exchange)
                tmp = path + ".tmp"
                merged.to_csv(tmp, index=False, encoding='utf-8-sig')
                os.replace(tmp, path)
                try:
                    del self.trade_calendar_df
                except Exception:
                    pass

    def get_trade_calendar(self, exchange: str = 'SSE', start_date: str = '20000101', end_date: str = None) -> pd.DataFrame:
        """
        获取交易日历（使用缓存+增量扩展）
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        self.ensure_calendar_coverage(start_date, end_date, exchange)
        df = self.trade_calendar_df
        mask = (df['cal_date'] >= int(start_date)) & (df['cal_date'] <= int(end_date))
        return df.loc[mask].copy()


    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str, save_local: bool = True) -> pd.DataFrame:
        """
        获取股票复权因子
        :param ts_code: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param save_local: 是否保存到本地
        :return: 复权因子DataFrame
        """
        df = self.provider.get_adj_factor(ts_code, start_date, end_date)
        
        if df is None or df.empty:
            # print(f"{ts_code} 没有获取到复权因子数据")
            return None
        
        # 保存到本地
        if save_local:
            file_path = os.path.join(self.adj_factor_path, f"{ts_code}.csv")
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            # print(f"复权因子数据已保存到：{file_path}")
        
        return df
    
    def ensure_last_year_complete(self, ts_code: str) -> pd.DataFrame:
        one_year_ago = datetime.now() - timedelta(days=365)
        start_date = one_year_ago.strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        self.ensure_calendar_coverage(start_date, end_date, 'SSE')
        local_df = self.load_local_data(ts_code)
        if local_df is None or local_df.empty:
            df = self.get_daily_k_data(ts_code, start_date=start_date, end_date=end_date, save_local=True)
            return df if df is not None else pd.DataFrame()
        df_range = local_df.copy()
        if 'trade_date' in df_range.columns:
            df_range = df_range[(df_range['trade_date'] >= pd.to_datetime(start_date)) & (df_range['trade_date'] <= pd.to_datetime(end_date))]
        missing = self.detect_missing_dates(exchange='SSE', start_date=start_date, end_date=end_date, df=df_range)
        if len(missing) > 0:
            new_df = self.get_daily_k_data(ts_code, start_date=start_date, end_date=end_date, save_local=False)
            if new_df is not None and not new_df.empty:
                merged = pd.concat([local_df, new_df], ignore_index=True)
                merged = merged.drop_duplicates(subset=['trade_date'], keep='last').sort_values(by='trade_date')
                file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
                merged.to_csv(file_path, index=False, encoding='utf-8-sig')
                return merged
        return local_df
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
            if start_date is None and end_date is None:
                ensured = self.ensure_last_year_complete(ts_code)
                return ensured
            else:
                # 如果指定了日期范围，检查本地数据是否覆盖该范围
                local_start = local_df['trade_date'].min().strftime('%Y%m%d')
                local_end = local_df['trade_date'].max().strftime('%Y%m%d')
                
                # 检查请求的日期范围是否完全在本地数据范围内
                if (start_date is None or start_date <= local_start) and (end_date is None or end_date >= local_end):
                    return local_df
        
        # 如果没有本地数据或本地数据不覆盖请求的日期范围，则从API获取
        # print(f"{ts_code}：本地数据不存在或不完整，从API获取")
        
        # 如果没有指定日期，默认获取最近一年的数据
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            # 计算一年前的日期
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        
        # 获取日线数据 (Delegate to Provider)
        df = self.provider.get_daily_k_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None or df.empty:
            # print(f"{ts_code} 没有获取到数据")
            return None
        
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
        self.ensure_calendar_coverage(start_date, end_date, exchange)
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
        min_date = missing_dates.min()
        max_date = missing_dates.max()

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
            # print(f"{ts_code}：传入的数据为空，跳过保存")
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
        # print(f"原始股价数据已保存到：{file_path}")

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
            # logger.debug(f"本地数据文件不存在：{file_path}")
            return None
    
    def fetch_all_stocks_last_year(self, limit: int = None):
        """
        爬取最近一年所有股票数据 (Batch Operation)
        """
        logger.info("开始爬取最近一年所有股票数据...")
        
        # 获取所有股票代码
        stock_codes = self.get_all_stock_codes()
        if limit and limit > 0:
            stock_codes = stock_codes[:limit]
            
        logger.info(f"共获取到{len(stock_codes)}只股票代码")
        
        # 计算一年前的日期
        one_year_ago = datetime.now() - timedelta(days=365)
        start_date = one_year_ago.strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        
        # 使用线程池并行获取数据
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(self.get_daily_k_data, ts_code, start_date, end_date, True): ts_code for ts_code in stock_codes}
            
            # 使用 tqdm 显示进度
            with tqdm(total=len(stock_codes), desc="Fetching Stocks", unit="stock") as pbar:
                for future in as_completed(future_to_stock):
                    ts_code = future_to_stock[future]
                    try:
                        future.result()
                        # logger.debug(f"{ts_code}数据获取成功")
                    except Exception as e:
                        logger.error(f"{ts_code}数据获取失败：{e}")
                    finally:
                        pbar.update(1)
        
        logger.info("所有股票数据获取完成")
    
    def update_stock_data(self, ts_code: str) -> None:
        """
        更新单只股票数据，仅获取新增交易日数据
        :param ts_code: 股票代码
        """
        # logger.info(f"开始更新{ts_code}数据...")
        
        # 加载本地数据
        local_df = self.load_local_data(ts_code)
        
        if local_df is None or local_df.empty:
            # 如果本地没有数据，获取最近一年的数据
            # logger.info(f"{ts_code}本地数据不存在，获取最近一年数据")
            self.get_daily_k_data(ts_code, save_local=True)
            return
        
        # 获取本地数据的最新日期
        latest_date = local_df['trade_date'].max()
        latest_date_str = latest_date.strftime('%Y%m%d')
        
        # 获取今天的日期
        today = datetime.now().strftime('%Y%m%d')
        
        if latest_date_str == today:
            # logger.info(f"{ts_code}数据已是最新，无需更新")
            return
        
        # 计算需要更新的日期范围
        # 从本地数据的最新日期加一天开始，到今天结束
        start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
        end_date = today
        
        # logger.info(f"{ts_code}：更新{start_date}至{end_date}的数据")
        
        # 获取新增数据
        new_data = self.get_daily_k_data(ts_code, start_date, end_date, save_local=False)
        
        if new_data is not None and not new_data.empty:
            # 合并数据
            combined_df = pd.concat([local_df, new_data])
            # 去重
            combined_df = combined_df.drop_duplicates(subset=['trade_date'])
            # 按日期排序
            combined_df = combined_df.sort_values(by=['trade_date'])
            # 保存合并后的数据
            file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
            combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            # logger.info(f"{ts_code}数据更新完成，新增{len(new_data)}条记录")
            
            # 同时更新复权因子
            self.get_adj_factor(ts_code, start_date, end_date, save_local=True)
        else:
            pass
            # logger.info(f"{ts_code}：没有获取到新增数据")
    
    def update_all_stocks_data(self, limit: int = None) -> None:
        """
        更新所有股票数据，仅获取新增交易日数据 (Batch Operation)
        """
        logger.info("开始更新所有股票数据...")
        
        # 获取所有股票代码
        stock_codes = self.get_all_stock_codes()
        if limit and limit > 0:
            stock_codes = stock_codes[:limit]
            
        logger.info(f"共获取到{len(stock_codes)}只股票代码")
        
        # 使用线程池并行更新数据
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(self.update_stock_data, ts_code): ts_code for ts_code in stock_codes}
            
            # 使用 tqdm 显示进度
            with tqdm(total=len(stock_codes), desc="Updating Stocks", unit="stock") as pbar:
                for future in as_completed(future_to_stock):
                    ts_code = future_to_stock[future]
                    try:
                        future.result()
                        # logger.debug(f"{ts_code}数据更新成功")
                    except Exception as e:
                        logger.error(f"{ts_code}数据更新失败：{e}")
                    finally:
                        pbar.update(1)
        
        logger.info("所有股票数据更新完成")
    
    def daily_update(self) -> None:
        """
        每日自动更新所有股票数据
        """
        logger.info("=== 开始每日自动更新股票数据 ===")
        
        # 获取当前日期
        today = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"更新日期：{today}")
        
        # 更新所有股票数据
        self.update_all_stocks_data()
        
        logger.info("=== 每日自动更新股票数据完成 ===")


# 示例用法
if __name__ == "__main__":
    # 使用 AkShare
    fetcher = StockDailyKLineFetcher(provider_name="akshare")
    # 或使用 Tushare (默认)
    # fetcher = StockDailyKLineFetcher()
    
    # 获取单只股票数据
    df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20240201')
    if df is not None:
        print(df.head())
    
    # 爬取最近一年所有股票数据
    # fetcher.fetch_all_stocks_last_year()
    
    # 更新单只股票数据
    # fetcher.update_stock_data('000001.SZ')
    
    # 更新所有股票数据
    # fetcher.update_all_stocks_data()
    
    # 每日自动更新
    # fetcher.daily_update()
