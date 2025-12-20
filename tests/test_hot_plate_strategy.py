#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试热门板块策略
"""

import sys
import os

# 将项目根目录添加到Python搜索路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.strategies.base_strategy import HotSectorKDJStrategy


def test_hot_plate_strategy():
    """
    测试热门板块策略
    """
    print("=== 测试热门板块策略 ===")
    
    # 初始化策略
    print("1. 初始化热门板块策略...")
    try:
        strategy = HotSectorKDJStrategy()
        print("   成功初始化热门板块策略")
        print(f"   热门行业列表：{strategy.hot_industries}")
    except Exception as e:
        print(f"   初始化策略失败：{e}")
        return
    
    # 初始化数据获取器
    fetcher = StockDailyKLineFetcher()
    
    # 测试热门板块判断
    print("\n2. 测试热门板块判断功能...")
    try:
        # 使用一些已知的科技股进行测试
        test_stocks = ['000063.SZ', '000977.SZ', '600588.SH', '000001.SZ']
        
        for ts_code in test_stocks:
            is_hot = strategy.is_hot_industry(ts_code)
            print(f"   {ts_code}: {is_hot}")
    except Exception as e:
        print(f"   测试热门板块判断失败：{e}")
    
    # 测试获取股票数据
    print("\n3. 测试获取股票数据...")
    try:
        # 获取少量股票数据用于测试
        test_stocks = ['000063.SZ', '000977.SZ', '600588.SH']
        start_date = '20251101'  # 前20天的数据
        end_date = '20251215'    # 截止到2025年12月15日
        
        print(f"   获取{test_stocks}的数据，时间范围：{start_date} 到 {end_date}")
        stocks_data = fetcher.get_multi_stocks_daily_k(test_stocks, start_date, end_date)
        print(f"   成功获取{len(stocks_data)}只股票的数据")
        
        # 测试策略筛选
        print("\n4. 测试策略筛选功能...")
        selected_stocks = strategy.filter_stocks(stocks_data)
        print(f"   符合热门板块策略的股票：{selected_stocks}")
        
        if selected_stocks:
            print("\n5. 详细信息：")
            for ts_code in selected_stocks:
                print(f"   - {ts_code}")
        
    except Exception as e:
        print(f"   测试策略筛选失败：{e}")
        import traceback
        traceback.print_exc()
    
    print("\n=== 热门板块策略测试完成 ===")


if __name__ == "__main__":
    test_hot_plate_strategy()