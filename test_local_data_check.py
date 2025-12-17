#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试本地数据检查功能
"""

import sys
import os

# 将项目根目录添加到Python搜索路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher


def test_local_data_check():
    """
    测试本地数据检查功能
    """
    print("=== 测试本地数据检查功能 ===")
    
    fetcher = StockDailyKLineFetcher()
    
    # 测试1：第一次获取数据，应该从API获取
    print("\n1. 第一次获取数据（应该从API获取）...")
    df1 = fetcher.get_daily_k_data('000001.SZ', start_date='20250101', end_date='20251215')
    if df1 is not None:
        print(f"   成功获取数据，共{len(df1)}行")
    
    # 测试2：第二次获取相同范围的数据，应该使用本地数据
    print("\n2. 第二次获取相同范围的数据（应该使用本地数据）...")
    df2 = fetcher.get_daily_k_data('000001.SZ', start_date='20250101', end_date='20251215')
    if df2 is not None:
        print(f"   成功获取数据，共{len(df2)}行")
    
    # 测试3：获取没有指定日期范围的数据，应该使用本地数据
    print("\n3. 获取没有指定日期范围的数据（应该使用本地数据）...")
    df3 = fetcher.get_daily_k_data('000001.SZ')
    if df3 is not None:
        print(f"   成功获取数据，共{len(df3)}行")
    
    # 测试4：获取超出本地数据范围的数据，应该从API获取
    print("\n4. 获取超出本地数据范围的数据（应该从API获取）...")
    df4 = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
    if df4 is not None:
        print(f"   成功获取数据，共{len(df4)}行")
    
    # 测试5：测试批量获取数据
    print("\n5. 测试批量获取数据...")
    stocks = ['000001.SZ', '600000.SH', '000858.SZ']
    result = fetcher.get_multi_stocks_daily_k(stocks, start_date='20250101', end_date='20251215')
    print(f"   成功获取{len(result)}只股票的数据")
    for ts_code, df in result.items():
        print(f"   {ts_code}：{len(df)}行数据")
    
    print("\n=== 测试完成 ===")


if __name__ == "__main__":
    test_local_data_check()