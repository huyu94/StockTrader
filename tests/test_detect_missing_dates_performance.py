import sys
import os
import pandas as pd
import timeit
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

def test_detect_missing_dates_performance():
    """
    测试detect_missing_dates方法的耗时
    """
    print("=== 测试detect_missing_dates方法耗时 ===")
    
    # 初始化数据获取器
    fetcher = StockDailyKLineFetcher()
    
    # 准备测试数据
    
    # 1. 模拟数据 - 带缺口
    sample_dates = [
        "2025-12-01", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05",
        "2025-12-08", "2025-12-09", "2025-12-10", 
        "2025-12-15", "2025-12-16", "2025-12-17"
    ]
    sample_df = pd.DataFrame({
        "trade_date": pd.to_datetime(sample_dates),
        "close": [10.0 + i*0.1 for i in range(len(sample_dates))]
    })
    
    # 2. 完整数据
    start_date = "20251201"
    end_date = "20251217"
    calendar_df = fetcher.get_trade_calendar(exchange='SSE', start_date=start_date, end_date=end_date)
    calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
    trade_df = calendar_df[calendar_df['is_open'] == 1]
    complete_df = pd.DataFrame({
        "trade_date": trade_df['cal_date'].tolist(),
        "close": [10.0 + i*0.1 for i in range(len(trade_df))]
    })
    
    # 测试不同场景下的耗时
    test_cases = [
        {"name": "模拟数据（带缺口）", "args": {"exchange": "SSE", "start_date": "20251201", "end_date": "20251217", "df": sample_df}},
        {"name": "完整数据", "args": {"exchange": "SSE", "start_date": "20251201", "end_date": "20251217", "df": complete_df}},
        {"name": "短期数据（5天）", "args": {"exchange": "SSE", "start_date": "20251201", "end_date": "20251205", "df": sample_df}},
        {"name": "长期数据（30天）", "args": {"exchange": "SSE", "start_date": "20251118", "end_date": "20251217", "df": sample_df}},
        {"name": "不提供df参数", "args": {"exchange": "SSE", "start_date": "20251201", "end_date": "20251210"}}
    ]
    
    # 执行测试
    for test_case in test_cases:
        name = test_case["name"]
        args = test_case["args"]
        
        print(f"\n--- 测试：{name} ---")
        print(f"参数：{args}")
        
        # 预热执行一次
        fetcher.detect_missing_dates(**args)
        
        # 测量耗时，执行10次取平均
        def run_test():
            fetcher.detect_missing_dates(**args)
        
        times = timeit.repeat(run_test, repeat=10, number=1)
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"执行10次平均耗时：{avg_time:.4f}秒")
        print(f"最快耗时：{min_time:.4f}秒")
        print(f"最慢耗时：{max_time:.4f}秒")
        print(f"所有耗时：{[f'{t:.4f}' for t in times]}")
    
    print("\n=== 耗时测试完成 ===")

if __name__ == "__main__":
    test_detect_missing_dates_performance()