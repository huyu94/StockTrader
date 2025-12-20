import sys
import os
import time
from datetime import datetime, timedelta
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.strategies.daily_recommend_strategy import DailyRecommendStrategy
from src.strategies.result_output import StockResultOutput
from project_var import OUTPUT_DIR

def run_once():
    fetcher = StockDailyKLineFetcher(provider_name="tushare")
    strategy = DailyRecommendStrategy()
    output = StockResultOutput(output_dir=OUTPUT_DIR)
    fetcher.get_stock_basic_info(save_local=True)
    fetcher.update_all_stocks_data()
    codes = fetcher.get_all_stock_codes()
    selected = []
    for ts_code in tqdm(codes, desc="每日筛选", unit="只"):
        df = fetcher.load_local_data(ts_code)
        if df is None or df.empty or len(df) < 15:
            continue
        try:
            if strategy.check_stock(ts_code, df):
                info = strategy.explain(ts_code, df)
                selected.append(info)
        except Exception:
            continue
    prefix = f"daily_main_{datetime.now().strftime('%Y%m%d')}"
    output.print_result([{
        "ts_code": s["ts_code"],
        "name": "",
        "industry": "",
        "exchange": "",
        "trade_date": s["trade_date"],
        "close": s["close"],
        "20d_gain": 0.0,
        "kdj": {"K": 0.0, "D": 0.0, "J": s["kdj_j"]},
        "macd": s["macd_hist"],
        "rsi": "",
        "bbi": ""
    } for s in selected], max_items=50)
    output.save_result(selected, formats=["csv", "json"], filename_prefix=prefix)

def next_run_time(now=None):
    if now is None:
        now = datetime.now()
    target = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now >= target:
        target = (now + timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    return target

if __name__ == "__main__":
    while True:
        now = datetime.now()
        target = next_run_time(now)
        delta = (target - now).total_seconds()
        time.sleep(max(0, delta))
        try:
            run_once()
        except Exception:
            pass
