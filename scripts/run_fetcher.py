import sys
import os
import argparse
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.fetchers.stock_fetcher import StockDailyKLineFetcher as StockFetcher
from src.data.loaders.stock_loader import StockLoader
from src.data.calendars.calendar_fetcher import CalendarFetcher
from datetime import datetime

def run(provider: str, limit: int = None, workers: int = 8):
    fetcher = StockFetcher(provider_name=provider)
    loader = StockLoader(provider_name=provider)
    cal_fetcher = CalendarFetcher(provider_name=provider)
    fetcher.get_stock_basic_info(save_local=True)
    codes = fetcher.get_all_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - pd.Timedelta(days=365)).strftime("%Y%m%d")
    cal_fetcher.ensure('SSE', start_date, end_date)
    summary = loader.check_missing_multi(codes, start_date, end_date, max_workers=workers)
    need = summary[summary["missing_count"] > 0]["ts_code"].tolist()
    if not need:
        return
    def do_fetch(code: str):
        info = loader.check_missing(code, start_date, end_date)
        if not info.is_need_fetch or not info.missing_dates:
            return
        s = min(info.missing_dates)
        e = max(info.missing_dates)
        fetcher.get_daily_k_data(code, s, e, save_local=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(do_fetch, code): code for code in need}
        with tqdm(total=len(need), desc="补齐缺失交易日数据", unit="只") as pbar:
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception:
                    pass
                finally:
                    pbar.update(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", type=str, default="tushare", choices=["tushare", "akshare"])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    run(args.provider, args.limit, args.workers)
