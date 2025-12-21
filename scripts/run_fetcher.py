import sys
import os
import argparse
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import setup_logger

from src.data.fetchers.stock_fetcher import StockDailyKLineFetcher as StockFetcher
from src.data.fetchers.base_info_fetcher import StockBaseInfoFetcher
from src.data.calendars.calendar_fetcher import CalendarFetcher
from datetime import datetime
import time
from src.data.providers.tushare_provider import TushareProvider
import json
from project_var import PROJECT_DIR
from src.data.missing.missing_matrix import MissingMatrixBuilder

def run(provider: str, limit: int = None, workers: int = 8, threshold: int = 1000):
    setup_logger()
    shared_provider = TushareProvider() if provider == "tushare" else None
    fetcher = StockFetcher(provider_name=provider, provider=shared_provider)
    base_info = StockBaseInfoFetcher(provider_name=provider, provider=shared_provider)
    t0 = time.time()
    base_info.get_stock_basic_info(exchanges=["SSE", "SZSE"], save_local=True)
    t_basic = time.time() - t0
    codes = base_info.get_all_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]
    cache_dir = os.path.join(PROJECT_DIR, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, "update_info.json")
    today = datetime.now().strftime("%Y%m%d")
    if os.path.exists(cache_path):
        try:
            info = json.load(open(cache_path, "r", encoding="utf-8"))
            if info.get("last_update_date") == today:
                last_count = int(info.get("count", 0))
                desired_count = len(codes)
                from loguru import logger
                if last_count >= desired_count:
                    logger.info(f"今日已更新，跳过抓取")
                    return
                else:
                    logger.info(f"今日已部分更新(count={last_count}<{desired_count})，继续抓取补齐")
        except Exception:
            pass
    cached_codes = set()
    if os.path.exists(cache_path):
        try:
            info = json.load(open(cache_path, "r", encoding="utf-8"))
            if info.get("last_update_date") == today and isinstance(info.get("codes"), list):
                cached_codes = set(info.get("codes"))
        except Exception:
            cached_codes = set()
    # 计算近一年窗口
    end_date = today
    start_dt = datetime.strptime(end_date, "%Y%m%d") - pd.Timedelta(days=365)
    start_date = start_dt.strftime("%Y%m%d")
    # 构建或加载缺失矩阵
    builder = MissingMatrixBuilder(provider_name=provider)
    counts, codes_map = builder.load_cache(start_date, end_date)
    if not counts:
        counts, codes_map = builder.build(codes=codes, start_date=start_date, end_date=end_date)
        builder.save_cache(start_date, end_date, counts, codes_map)
    # 先按交易日批量抓取（大缺口优先）
    from loguru import logger
    dates_sorted = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    bulk_written_codes = set()
    for d, cnt in dates_sorted:
        if cnt >= threshold:
            miss_codes = codes_map.get(d, set())
            if not miss_codes:
                continue
            written = fetcher.fetch_by_date(trade_date=d, codes=miss_codes)
            bulk_written_codes.update(written)
    # 再做单股补齐（小缺口）
    remain_codes = [c for c in codes if c not in cached_codes]
    # 逐代码计算缺口最早日期，按线程池并发抓取
    def task(code: str):
        try:
            miss = fetcher.detect_missing_dates(ts_code=code, end_date=end_date, window_days=365)
            if not miss:
                return False
            start_for_code = min(miss)
            df = fetcher.fetch_one(ts_code=code, start_date=start_for_code, end_date=end_date, save_local=True)
            return df is not None and not df.empty
        except Exception:
            return False
    t3 = time.time()
    successes = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(task, c): c for c in remain_codes}
        with tqdm(total=len(remain_codes), desc="单股补齐", unit="只") as pbar:
            for fut in as_completed(futures):
                try:
                    ok = fut.result()
                    if ok:
                        successes.append(futures[fut])
                finally:
                    pbar.update(1)
    t_fetch = time.time() - t3
    logger.info(f"耗时统计 basic={t_basic:.2f}s bulk_codes={len(bulk_written_codes)} single_fetched={len(successes)}")
    try:
        new_codes = list(set(list(cached_codes) + successes + list(bulk_written_codes)))
        new_count = len(new_codes)
        json.dump({"last_update_date": today, "count": new_count, "codes": new_codes}, open(cache_path, "w", encoding="utf-8"))
    except Exception:
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", type=str, default="tushare", choices=["tushare", "akshare"])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--threshold", type=int, default=1000)
    args = parser.parse_args()
    run(args.provider, args.limit, args.workers, args.threshold)
