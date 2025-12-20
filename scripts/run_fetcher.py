import sys
import os
import argparse
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

def run(provider: str, limit: int = None):
    fetcher = StockDailyKLineFetcher(provider_name=provider)
    fetcher.get_stock_basic_info(save_local=True)
    codes = fetcher.get_all_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]
    for ts_code in tqdm(codes, desc="拉取并补齐近一年", unit="只"):
        try:
            fetcher.ensure_last_year_complete(ts_code)
        except Exception:
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", type=str, default="tushare", choices=["tushare", "akshare"])
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(args.provider, args.limit)

