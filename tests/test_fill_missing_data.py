import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

# 测试自动补爬缺失数据功能
if __name__ == "__main__":
    fetcher = StockDailyKLineFetcher()
    
    # 测试1：检测并补爬单只股票的缺失数据
    print("\n=== 测试1：检测并补爬单只股票的缺失数据 ===")
    # 选择一个有缺失数据的股票进行测试
    test_code = "000001.SZ"
    
    # 首先检测缺失数据
    print(f"\n1. 检测{test_code}的缺失数据")
    missing_dates = fetcher.detect_missing_dates(
        ts_code=test_code, 
        start_date="20251201", 
        end_date="20251216"
    )
    
    # 如果有缺失数据，则尝试补爬
    if len(missing_dates) > 0:
        print(f"\n2. 补爬{test_code}的缺失数据")
        fetcher.fill_missing_data(
            ts_code=test_code, 
            start_date="20251201", 
            end_date="20251216"
        )
        
        # 补爬后再次检测，验证是否补爬成功
        print(f"\n3. 再次检测{test_code}的缺失数据")
        new_missing_dates = fetcher.detect_missing_dates(
            ts_code=test_code, 
            start_date="20251201", 
            end_date="20251216"
        )
        
        if len(new_missing_dates) == 0:
            print(f"✅ 成功补爬所有缺失数据")
        else:
            print(f"❌ 仍有{len(new_missing_dates)}条缺失数据未补爬成功")
            print(f"未补爬的日期：{new_missing_dates.strftime('%Y-%m-%d').tolist()}")
    else:
        print(f"{test_code}没有缺失数据，无需补爬")
    
    print("\n=== 所有测试完成 ===")