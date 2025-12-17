import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

def test_detect_missing_dates():
    """
    测试检测缺失交易日的功能
    """
    print("=== 测试detect_missing_dates函数 ===")
    
    # 初始化数据获取器
    fetcher = StockDailyKLineFetcher()
    
    # 测试1：使用示例数据检测缺失日期
    print("\n--- 测试1：使用模拟数据检测缺失日期 ---")
    
    # 创建模拟数据，模拟有缺口的数据（缺少2025-12-11至2025-12-14的数据）
    sample_dates = [
        "2025-12-01", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05",
        "2025-12-08", "2025-12-09", "2025-12-10", 
        # 缺少 2025-12-11 至 2025-12-14
        "2025-12-15", "2025-12-16", "2025-12-17"
    ]
    
    # 创建模拟DataFrame
    df = pd.DataFrame({
        "trade_date": pd.to_datetime(sample_dates),
        "close": [10.0 + i*0.1 for i in range(len(sample_dates))]
    })
    
    print(f"模拟数据包含 {len(df)} 个交易日")
    print(f"模拟数据日期范围：{df['trade_date'].min().strftime('%Y-%m-%d')} 至 {df['trade_date'].max().strftime('%Y-%m-%d')}")
    
    # 检测缺失日期
    missing_dates = fetcher.detect_missing_dates(
        start_date="20251201",
        end_date="20251217",
        df=df
    )
    
    print(f"检测到 {len(missing_dates)} 个缺失交易日")
    
    # 测试2：检测本地数据的缺失日期
    print("\n--- 测试2：检测本地数据的缺失日期 ---")
    
    # 确保本地有数据可检测
    ts_code = "000001.SZ"
    
    # 先获取一些数据保存到本地
    print(f"先获取{ts_code}的一些数据保存到本地...")
    df_local = fetcher.get_daily_k_data(
        ts_code=ts_code,
        start_date="20251201",
        end_date="20251210",
        save_local=True
    )
    
    # 检测缺失日期
    missing_dates_local = fetcher.detect_missing_dates(
        ts_code=ts_code,
        start_date="20251201",
        end_date="20251217"
    )
    
    print(f"检测到 {len(missing_dates_local)} 个缺失交易日")
    
    # 测试3：检测完整数据的缺失日期（应该没有缺失）
    print("\n--- 测试3：检测完整数据的缺失日期 ---")
    
    # 获取完整的交易日序列
    calendar_df = fetcher.get_trade_calendar(exchange='SSE', start_date="20251201", end_date="20251217")
    calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
    trade_df = calendar_df[calendar_df['is_open'] == 1]
    
    # 创建完整的模拟数据
    complete_dates = trade_df['cal_date'].tolist()
    df_complete = pd.DataFrame({
        "trade_date": complete_dates,
        "close": [10.0 + i*0.1 for i in range(len(complete_dates))]
    })
    
    print(f"完整数据包含 {len(df_complete)} 个交易日")
    
    # 检测缺失日期
    missing_dates_complete = fetcher.detect_missing_dates(
        start_date="20251201",
        end_date="20251217",
        df=df_complete
    )
    
    print(f"检测到 {len(missing_dates_complete)} 个缺失交易日")
    
    if len(missing_dates_complete) == 0:
        print("✅ 检测结果正确：完整数据没有缺失交易日")
    else:
        print("❌ 检测结果错误：完整数据不应有缺失交易日")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    test_detect_missing_dates()