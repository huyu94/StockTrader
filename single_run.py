import sys
import os
import argparse
from datetime import datetime
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.strategies.registry import get_strategy, available_strategies
from src.strategies.result_output import StockResultOutput
from project_var import OUTPUT_DIR, DATA_DIR

def run(provider: str, concepts: str = None, industries: str = None, limit: int = None, strategy_name: str = "少妇战法"):
    fetcher = StockDailyKLineFetcher(provider_name=provider)
    strategy = get_strategy(strategy_name)
    output = StockResultOutput(output_dir=OUTPUT_DIR)
    fetcher.get_stock_basic_info(save_local=True)
    fetcher.update_all_stocks_data(limit=limit)
    codes = fetcher.get_all_stock_codes()
    if concepts:
        names = [c.strip() for c in concepts.split(',') if c.strip()]
        concept_codes = set(fetcher.get_stocks_by_concepts(names))
        if concept_codes:
            codes = [c for c in codes if c in concept_codes]
    if industries:
        inds = [i.strip() for i in industries.split(',') if i.strip()]
        basic_path = os.path.join(DATA_DIR, "stock_basic_info.csv")
        if os.path.exists(basic_path):
            import pandas as pd
            basic_df = pd.read_csv(basic_path)
            if 'industry' in basic_df.columns:
                allowed = set(basic_df[basic_df['industry'].fillna('').apply(lambda x: any(k.lower() in str(x).lower() for k in inds))]['ts_code'].tolist())
                codes = [c for c in codes if c in allowed]
    if limit and limit > 0:
        codes = codes[:limit]
    selected = []
    for ts_code in tqdm(codes, desc="单次筛选", unit="只"):
        df = fetcher.load_local_data(ts_code)
        if df is None or df.empty or len(df) < 15:
            continue
        try:
            if hasattr(strategy, "check_stock") and strategy.check_stock(ts_code, df):
                info = strategy.explain(ts_code, df)
                selected.append(info)
        except Exception:
            continue
    prefix = f"single_run_{provider}_{datetime.now().strftime('%Y%m%d')}"
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", type=str, default="tushare", choices=["akshare", "tushare"])
    parser.add_argument("--concepts", type=str, default=None, help="按概念筛选（暂不启用）")
    parser.add_argument("--industries", type=str, default=None, help="按行业筛选，逗号分隔，例如: 生物医药,人工智能,电力")
    parser.add_argument("--limit", type=int, default=None, help="限制处理的股票数量，不指定则全市场")
    parser.add_argument("--strategy", type=str, default="少妇战法", choices=available_strategies())
    args = parser.parse_args()
    run(args.provider, None, args.industries, args.limit, args.strategy)
