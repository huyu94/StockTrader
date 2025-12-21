import sys
import os
import argparse
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

"""
策略运行脚本流程：
1) 初始化全局日志
2) 获取策略实例与数据入口（基础信息、数据加载器）
3) 确保并读取基础信息，构建代码列表与名称映射
4) 并行遍历股票：
   - 加载本地日线数据
   - 调用策略的 check_stock 命中则调用 explain 生成记录
5) 输出结果到终端与 CSV/JSON
"""

from src.managers.data_manager import Manager
from src.strategies.registry import get_strategy, available_strategies
from src.strategies.result_output import StockResultOutput
from project_var import OUTPUT_DIR, DATA_DIR, PROJECT_DIR
from config import setup_logger

def run(strategy_name: str, industries: str = None, limit: int = None, workers: int = 8):
    # 1) 初始化日志（终端 INFO，文件 DEBUG）
    setup_logger()
    strategy = get_strategy(strategy_name)
    
    # 使用统一的 Manager
    data_manager = Manager()
    
    output = StockResultOutput(output_dir=OUTPUT_DIR)
    # 2) 确保基础信息存在（首次运行会落盘）
    basic_path = os.path.join(DATA_DIR, "basic_info.csv") # Manager uses SQLite database now
    if not os.path.exists(basic_path):
        data_manager.update_basic_info()
    
    basic_df = data_manager.all_basic_info
    # 3) 读取代码列表并按行业过滤（可选）
    codes = basic_df["ts_code"].tolist() if basic_df is not None else []
    
    if industries:
        inds = [i.strip() for i in industries.split(',') if i.strip()]
        if 'industry' in basic_df.columns:
            allowed = set(basic_df[basic_df['industry'].fillna('').apply(lambda x: any(k.lower() in str(x).lower() for k in inds))]['ts_code'].tolist())
            codes = [c for c in codes if c in allowed]
    if limit and limit > 0:
        codes = codes[:limit]
    # 4) 构建名称映射，便于结果展示
    name_map = {}
    if basic_df is not None and 'ts_code' in basic_df.columns and 'name' in basic_df.columns:
        name_map = dict(zip(basic_df['ts_code'], basic_df['name']))
    selected = []
    # 5) 并行工作函数：加载本地数据 → 策略判定 → 生成记录
    def work(code: str):
        # 使用 Manager 的 storage 直接读取 (或者 data_manager.daily_storage.load(code))
        df = data_manager.daily_storage.load(code)
        if df is None or df.empty or len(df) < 15:
            return None, None
        try:
            picked = None
            if strategy.check_stock(code, df):
                picked = strategy.explain(code, df)
            return picked, None
        except Exception:
            return None, None
    # 6) 并行执行遍历
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(work, c): c for c in codes}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="运行策略(并行)", unit="只"):
            picked, _ = fut.result()
            if picked:
                selected.append(picked)
    # 7) 增补名称并输出结果
    selected = [{**s, "name": name_map.get(s.get("ts_code", ""), "")} for s in selected]
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
