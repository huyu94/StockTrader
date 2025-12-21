import sys
import os
import argparse
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.fetchers.stock_fetcher import StockDailyKLineFetcher
from src.strategies.registry import get_strategy, available_strategies
from src.strategies.result_output import StockResultOutput
from project_var import OUTPUT_DIR, DATA_DIR, PROJECT_DIR

def run(strategy_name: str, industries: str = None, limit: int = None, workers: int = 8):
    strategy = get_strategy(strategy_name)
    fetcher = StockDailyKLineFetcher(provider_name="tushare")
    output = StockResultOutput(output_dir=OUTPUT_DIR)
    logs_dir = os.path.join(PROJECT_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"run_strategies_{strategy_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    logger.add(log_file, level="DEBUG", encoding="utf-8")
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
    def work(code: str):
        df = fetcher.load_local_data(code)
        if df is None or df.empty or len(df) < 15:
            return None, None
        try:
            picked = None
            if strategy.check_stock(code, df):
                picked = strategy.explain(code, df)
            try:
                df_pre = None
                try:
                    df_pre = strategy.preprocess(df)
                except Exception:
                    df_pre = None
                trd = df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in df.columns and len(df) > 0 else ''
                if df_pre is not None and not df_pre.empty:
                    close = float(df_pre['收盘价'].iloc[-1]) if '收盘价' in df_pre.columns else 0.0
                    j_prev = float(df_pre['J'].iloc[-2]) if 'J' in df_pre.columns and len(df_pre) >= 2 else float('nan')
                    j_curr = float(df_pre['J'].iloc[-1]) if 'J' in df_pre.columns else float('nan')
                else:
                    close = 0.0
                    j_prev = float('nan')
                    j_curr = float('nan')
                passed = picked is not None
                logger.debug(f"{code},{trd},close={close},J_prev={j_prev},J={j_curr},passed={passed}")
            except Exception:
                pass
            return picked, None
        except Exception:
            return None, None
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(work, c): c for c in codes}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="运行策略(并行)", unit="只"):
            picked, _ = fut.result()
            if picked:
                selected.append(picked)
    prefix = f"run_strategies_{strategy_name}_{datetime.now().strftime('%Y%m%d')}"
    output.print_result([{
        "ts_code": s.get("ts_code", ""),
        "name": "",
        "industry": "",
        "exchange": "",
        "trade_date": s.get("trade_date", ""),
        "close": s.get("close", 0.0),
        "20d_gain": 0.0,
        "kdj": {"K": 0.0, "D": 0.0, "J": s.get("kdj_j", 0.0)},
        "macd": s.get("macd_hist", 0.0),
        "rsi": "",
        "bbi": ""
    } for s in selected], max_items=50)
    paths = output.save_result(selected, formats=["csv", "json"], filename_prefix=prefix)
    for p in paths:
        print(p)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", type=str, default="少妇战法", choices=available_strategies())
    parser.add_argument("--industries", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    run(args.strategy, args.industries, args.limit, args.workers)
