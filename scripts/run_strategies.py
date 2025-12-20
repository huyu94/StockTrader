import sys
import os
import argparse
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.fetchers.stock_fetcher import StockDailyKLineFetcher
from src.strategies.registry import get_strategy, available_strategies
from src.strategies.result_output import StockResultOutput
from project_var import OUTPUT_DIR, DATA_DIR

def run(strategy_name: str, industries: str = None, limit: int = None, debug: bool = False):
    strategy = get_strategy(strategy_name)
    fetcher = StockDailyKLineFetcher(provider_name="tushare")
    output = StockResultOutput(output_dir=OUTPUT_DIR)
    basic_path = os.path.join(DATA_DIR, "stock_basic_info.csv")
    if not os.path.exists(basic_path):
        fetcher.get_stock_basic_info(save_local=True)
    codes = fetcher.get_all_stock_codes()
    if industries:
        inds = [i.strip() for i in industries.split(',') if i.strip()]
        df = pd.read_csv(basic_path)
        if 'industry' in df.columns:
            allowed = set(df[df['industry'].fillna('').apply(lambda x: any(k.lower() in str(x).lower() for k in inds))]['ts_code'].tolist())
            codes = [c for c in codes if c in allowed]
    if limit and limit > 0:
        codes = codes[:limit]
    selected = []
    debug_rows = []
    for ts_code in tqdm(codes, desc="运行策略", unit="只"):
        df = fetcher.load_local_data(ts_code)
        if df is None or df.empty or len(df) < 15:
            continue
        try:
            if strategy.check_stock(ts_code, df):
                info = strategy.explain(ts_code, df)
                selected.append(info)
            if debug and hasattr(strategy, "debug_check"):
                try:
                    row = strategy.debug_check(ts_code, df)
                    debug_rows.append(row)
                except Exception as e:
                    logger.error(f"调试失败 {ts_code}: {e}")
                    trd = df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in df.columns and len(df) > 0 else ''
                    debug_rows.append({"ts_code": ts_code, "trade_date": trd, "error": str(e), "passed": False})
        except Exception:
            continue
    prefix = f"run_strategies_{strategy_name}_{datetime.now().strftime('%Y%m%d')}"
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
    paths = output.save_result(selected, formats=["csv", "json"], filename_prefix=prefix)
    for p in paths:
        print(p)
    if debug and debug_rows:
        df_dbg = pd.DataFrame(debug_rows)
        dbg_name = f"debug_{strategy_name}_{datetime.now().strftime('%Y%m%d')}.csv"
        dbg_path = os.path.join(OUTPUT_DIR, dbg_name)
        df_dbg.to_csv(dbg_path, index=False, encoding='utf-8')
        print(dbg_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", type=str, default="少妇战法", choices=available_strategies())
    parser.add_argument("--industries", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    run(args.strategy, args.industries, args.limit, args.debug)
