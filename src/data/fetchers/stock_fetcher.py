from loguru import logger
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from functools import cached_property

from project_var import DATA_DIR, PROJECT_DIR
from src.data.common.column_mappings import DAILY_COLUMN_MAPPINGS, ADJ_FACTOR_COLUMN_MAPPINGS
from src.data.providers.base import BaseProvider
from src.data.providers.tushare_provider import TushareProvider
from src.data.providers.akshare_provider import AkShareProvider

class StockDailyKLineFetcher:

    def __init__(self, provider_name: str = "tushare"):
        self.stock_data_path = os.path.join(DATA_DIR, "stock_data")
        self.adj_factor_path = os.path.join(DATA_DIR, "adj_factor")
        self.stock_basic_info_path = os.path.join(DATA_DIR, "stock_basic_info.csv")
        self.trade_calendar_path = os.path.join(DATA_DIR, "trade_calendar.csv")
        self.calendar_cache_dir = os.path.join(PROJECT_DIR, "cache")
        os.makedirs(self.calendar_cache_dir, exist_ok=True)

        os.makedirs(self.stock_data_path, exist_ok=True)
        os.makedirs(self.adj_factor_path, exist_ok=True)
        
        if provider_name == "akshare":
            self.provider: BaseProvider = AkShareProvider()
        else:
            self.provider: BaseProvider = TushareProvider()

    def get_stock_basic_info(self, exchange: str = 'SSE', save_local: bool = True) -> pd.DataFrame:
        df = self.provider.get_stock_basic_info()
        logger.info(f"获取股票基本信息完成，共{len(df)}条记录")
        if exchange and 'exchange' in df.columns:
             filtered_df = df[df['exchange'] == exchange]
             if not filtered_df.empty:
                 df = filtered_df
        if save_local:
            self.save_stock_basic_info(df)
        return df

    def save_stock_basic_info(self, df: pd.DataFrame):
        if os.path.exists(self.stock_basic_info_path):
            origin_df = pd.read_csv(self.stock_basic_info_path)
            merged_df = pd.concat([origin_df, df]).drop_duplicates(subset=['ts_code'], keep='last')
        else:
            merged_df = df.copy()
        merged_df.to_csv(self.stock_basic_info_path, index=False, encoding='utf-8-sig')
        logger.info(f"股票基本信息已保存到：{self.stock_basic_info_path}")

    def get_all_stock_codes(self) -> list:
        if os.path.exists(self.stock_basic_info_path):
            df = pd.read_csv(self.stock_basic_info_path)
            return df['ts_code'].tolist()
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
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        self.ensure_calendar_coverage(start_date, end_date, exchange)
        df = self.trade_calendar_df
        mask = (df['cal_date'] >= int(start_date)) & (df['cal_date'] <= int(end_date))
        return df.loc[mask].copy()

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str, save_local: bool = True) -> pd.DataFrame:
        df = self.provider.get_adj_factor(ts_code, start_date, end_date)
        if df is None or df.empty:
            return None
        if save_local:
            file_path = os.path.join(self.adj_factor_path, f"{ts_code}.csv")
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
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
        local_df = self.load_local_data(ts_code)
        if local_df is not None:
            if start_date is None and end_date is None:
                ensured = self.ensure_last_year_complete(ts_code)
                return ensured
            else:
                local_start = local_df['trade_date'].min().strftime('%Y%m%d')
                local_end = local_df['trade_date'].max().strftime('%Y%m%d')
                if (start_date is None or start_date <= local_start) and (end_date is None or end_date >= local_end):
                    return local_df
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        df = self.provider.get_daily_k_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return None
        if save_local:
            self.save_to_local_data(ts_code, df)
        self.get_adj_factor(ts_code, start_date, end_date, save_local)
        return df

    def detect_missing_dates(self, exchange: str = 'SSE', start_date: str = None, end_date: str = None, df: pd.DataFrame = None) -> pd.DatetimeIndex:
        logger.debug("开始检测缺失交易日")
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        logger.debug(f"检测区间：{start_date} 至 {end_date}")
        self.ensure_calendar_coverage(start_date, end_date, exchange)
        calendar_df = self.get_trade_calendar(exchange=exchange, start_date=start_date, end_date=end_date)
        calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
        trade_df = calendar_df[calendar_df['is_open'] == 1]
        trade_dates = pd.DatetimeIndex(trade_df['cal_date'].tolist())
        if df is None or df.empty:
            existing_dates = []
        else:
            existing_dates = pd.DatetimeIndex(df['trade_date'].tolist())
        missing_dates = trade_dates.difference(existing_dates)
        missing_dates = missing_dates.sort_values()
        logger.debug(f"总应爬交易日数：{len(trade_dates)}")
        logger.debug(f"已爬交易日数：{len(existing_dates)}")
        logger.debug(f"缺失交易日数：{len(missing_dates)}")
        if len(missing_dates) > 0:
            logger.debug(f"缺失日期列表：{missing_dates.strftime('%Y-%m-%d').tolist()}")
        return missing_dates
    
    def detect_missing_dates_multi(self, ts_codes: list, exchange: str = 'SSE', start_date: str = None, end_date: str = None, max_workers: int = 10) -> pd.DataFrame:
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        self.ensure_calendar_coverage(start_date, end_date, exchange)
        cal = self.get_trade_calendar(exchange=exchange, start_date=start_date, end_date=end_date)
        cal['cal_date'] = pd.to_datetime(cal['cal_date'], format='%Y%m%d')
        trade_dates = pd.DatetimeIndex(cal[cal['is_open'] == 1]['cal_date'].tolist())
        results = []
        def work(code: str):
            df_local = self.load_local_data(code)
            if df_local is None or df_local.empty or 'trade_date' not in df_local.columns:
                miss = len(trade_dates)
                return {"ts_code": code, "missing_count": miss}
            df_range = df_local[(df_local['trade_date'] >= pd.to_datetime(start_date)) & (df_local['trade_date'] <= pd.to_datetime(end_date))]
            existing = pd.DatetimeIndex(df_range['trade_date'].tolist())
            miss = len(trade_dates.difference(existing))
            return {"ts_code": code, "missing_count": miss}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(work, c): c for c in ts_codes}
            for fut in as_completed(futures):
                try:
                    results.append(fut.result())
                except Exception:
                    results.append({"ts_code": futures[fut], "missing_count": -1})
        return pd.DataFrame(results)
    
    def fill_missing_data(self, ts_code: str, start_date: str = None, end_date: str = None) -> None:
        print(f"=== 开始补爬{ts_code}缺失数据 ===")
        missing_dates = self.detect_missing_dates(ts_code, start_date, end_date)
        min_date = missing_dates.min()
        max_date = missing_dates.max()
        if len(missing_dates) == 0:
            print(f"{ts_code} 没有缺失数据，无需补爬")
            return
        missing_dates_str = missing_dates.strftime('%Y%m%d').tolist()
        missing_dates_str.sort()
        start_missing = missing_dates_str[0]
        end_missing = missing_dates_str[-1]
        print(f"开始补爬{start_missing}至{end_missing}的缺失数据...")
        missing_df = self.get_daily_k_data(ts_code, start_missing, end_missing, save_local=False)
        if missing_df is not None and not missing_df.empty:
            print(f"成功获取{len(missing_df)}条缺失数据")
            existing_df = self.load_local_data(ts_code)
            combined_df = pd.concat([existing_df, missing_df])
            combined_df = combined_df.drop_duplicates(subset=['trade_date'])
            combined_df = combined_df.sort_values(by=['trade_date'])
            file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
            combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"已将补爬数据合并到本地文件：{file_path}")
            print(f"合并后数据共{len(combined_df)}条")
        else:
            print("未获取到缺失数据")
        print(f"=== {ts_code}缺失数据补爬完成 ===")

    def save_to_local_data(self, ts_code: str, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        origin_df = self.load_local_data(ts_code)
        if origin_df is None or origin_df.empty:
            merged_df = df.copy()
        else:
            merged_df = pd.concat([origin_df, df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=['trade_date'], keep='last')
        merged_df = merged_df.sort_values(by='trade_date')
        merged_df.to_csv(file_path, index=False, encoding='utf-8-sig')

    def load_local_data(self, ts_code: str) -> Union[pd.DataFrame, None]:
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df
        else:
            return None
    
    def fetch_all_stocks_last_year(self, limit: int = None):
        logger.info("开始爬取最近一年所有股票数据...")
        stock_codes = self.get_all_stock_codes()
        if limit and limit > 0:
            stock_codes = stock_codes[:limit]
        logger.info(f"共获取到{len(stock_codes)}只股票代码")
        one_year_ago = datetime.now() - timedelta(days=365)
        start_date = one_year_ago.strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(self.get_daily_k_data, ts_code, start_date, end_date, True): ts_code for ts_code in stock_codes}
            with tqdm(total=len(stock_codes), desc="Fetching Stocks", unit="stock") as pbar:
                for future in as_completed(future_to_stock):
                    ts_code = future_to_stock[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"{ts_code}数据获取失败：{e}")
                    finally:
                        pbar.update(1)
        logger.info("所有股票数据获取完成")
    
    def update_stock_data(self, ts_code: str) -> None:
        local_df = self.load_local_data(ts_code)
        if local_df is None or local_df.empty:
            self.get_daily_k_data(ts_code, save_local=True)
            return
        latest_date = local_df['trade_date'].max()
        latest_date_str = latest_date.strftime('%Y%m%d')
        today = datetime.now().strftime('%Y%m%d')
        if latest_date_str == today:
            return
        start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
        end_date = today
        new_data = self.get_daily_k_data(ts_code, start_date, end_date, save_local=False)
        if new_data is not None and not new_data.empty:
            combined_df = pd.concat([local_df, new_data])
            combined_df = combined_df.drop_duplicates(subset=['trade_date'])
            combined_df = combined_df.sort_values(by=['trade_date'])
            file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
            combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            self.get_adj_factor(ts_code, start_date, end_date, save_local=True)
        else:
            pass
    
    def update_all_stocks_data(self, limit: int = None) -> None:
        logger.info("开始更新所有股票数据...")
        stock_codes = self.get_all_stock_codes()
        if limit and limit > 0:
            stock_codes = stock_codes[:limit]
        logger.info(f"共获取到{len(stock_codes)}只股票代码")
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(self.update_stock_data, ts_code): ts_code for ts_code in stock_codes}
            with tqdm(total=len(stock_codes), desc="Updating Stocks", unit="stock") as pbar:
                for future in as_completed(future_to_stock):
                    ts_code = future_to_stock[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"{ts_code}数据更新失败：{e}")
                    finally:
                        pbar.update(1)
        logger.info("所有股票数据更新完成")
    
    def daily_update(self) -> None:
        logger.info("=== 开始每日自动更新股票数据 ===")
        today = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"更新日期：{today}")
        self.update_all_stocks_data()
        logger.info("=== 每日自动更新股票数据完成 ===")
