import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDataFetcher

# 测试缺失日期检测功能
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 测试1：使用示例数据检测缺失日期
    print("\n=== 测试1：使用示例数据检测缺失日期 ===")
    # 创建示例数据，模拟有缺口的数据（缺少2025-12-11至2025-12-14的数据）
    sample_df = pd.DataFrame({
        "trade_date": pd.to_datetime([
            "2025-12-01", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05",
            "2025-12-08", "2025-12-09", "2025-12-10", "2025-12-15", "2025-12-16"
        ]),
        "open": [11.6, 11.68, 11.64, 11.55, 11.49, 11.49, 11.52, 11.44, 11.3, 11.35],
        "close": [11.69, 11.64, 11.55, 11.49, 11.53, 11.52, 11.43, 11.33, 11.35, 11.4]
    })
    print("示例数据:")
    print(sample_df)
    
    # 检测缺失日期
    missing_dates = fetcher.detect_missing_dates(
        start_date="20251201", 
        end_date="20251216", 
        df=sample_df
    )
    
    # 测试2：检测本地股票数据的缺失日期
    print("\n=== 测试2：检测本地股票数据的缺失日期 ===")
    missing_dates = fetcher.detect_missing_dates(
        ts_code="000001.SZ", 
        start_date="20251201", 
        end_date="20251216"
    )
    
    # 测试3：使用增量检测
    print("\n=== 测试3：使用增量检测 ===")
    # 假设上次爬取到2025-12-10，这次爬取到2025-12-16，检测中间的缺口
    sample_df2 = pd.DataFrame({
        "trade_date": pd.to_datetime(["2025-12-15", "2025-12-16"]),
        "open": [11.3, 11.35],
        "close": [11.35, 11.4]
    })
    print("新增数据:")
    print(sample_df2)
    
    # 检测增量数据的缺失日期（上次结束日+1到本次开始日-1）
    missing_dates = fetcher.detect_missing_dates(
        start_date="20251211", 
        end_date="20251214", 
        df=sample_df2
    )
    
    print("\n=== 所有测试完成 ===")