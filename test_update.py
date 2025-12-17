#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本，用于验证系统更新后的功能
"""

import sys
import os

# 将项目根目录添加到Python搜索路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.indicators.technical_indicators import TechnicalIndicators


def test_data_fetch():
    """
    测试数据获取功能
    """
    print("=== 测试数据获取功能 ===")
    
    fetcher = StockDailyKLineFetcher()
    
    # 测试获取单只股票数据
    print("\n1. 测试获取单只股票数据...")
    try:
        df = fetcher.get_daily_k_data('000001.SZ', start_date='20250101', end_date='20251215', save_local=False)
        if df is not None:
            print(f"   成功获取000001.SZ的数据，共{len(df)}行")
            print("   数据列名：")
            print(f"   {list(df.columns)}")
            print("   数据示例：")
            print(df.head())
        else:
            print("   获取数据失败")
    except Exception as e:
        print(f"   获取数据失败：{e}")
    
    # 测试获取所有股票代码
    print("\n2. 测试获取所有股票代码...")
    try:
        stock_codes = fetcher.get_all_stock_codes()
        print(f"   成功获取{len(stock_codes)}只股票代码")
        print(f"   示例股票代码：{stock_codes[:5]}")
    except Exception as e:
        print(f"   获取股票代码失败：{e}")


def test_indicators_calculation():
    """
    测试指标计算功能
    """
    print("\n=== 测试指标计算功能 ===")
    
    # 创建模拟数据
    import pandas as pd
    import numpy as np
    
    # 创建模拟的股票数据
    dates = pd.date_range(start='20250101', periods=30)
    np.random.seed(42)
    
    data = {
        '日期': dates,
        '开盘价': np.random.rand(30) * 10 + 100,
        '收盘价': np.random.rand(30) * 10 + 100,
        '最高价': np.random.rand(30) * 10 + 105,
        '最低价': np.random.rand(30) * 10 + 95,
        '成交量': np.random.randint(1000000, 10000000, 30),
        '成交额': np.random.rand(30) * 100000000 + 100000000
    }
    
    df = pd.DataFrame(data)
    print(f"创建模拟数据，共{len(df)}行")
    print("数据示例：")
    print(df.head())
    
    # 测试指标计算
    calculator = TechnicalIndicators()
    
    # 测试计算所有指标
    print("\n测试计算所有指标...")
    try:
        df_with_indicators = calculator.calculate_all_indicators(df)
        print(f"成功计算所有指标")
        print(f"指标计算后的数据列名：")
        print(f"{list(df_with_indicators.columns)}")
        print("技术指标示例：")
        indicator_cols = ['日期', 'BBI', 'MACD', 'DEA', 'MACD_HIST', 'RSI', 'K', 'D', 'J']
        print(df_with_indicators[indicator_cols].tail())
    except Exception as e:
        print(f"计算指标失败：{e}")
        import traceback
        traceback.print_exc()


def test_batch_fetch():
    """
    测试批量获取股票数据
    """
    print("\n=== 测试批量获取股票数据 ===")
    
    fetcher = StockDailyKLineFetcher()
    
    # 测试获取少量股票数据
    print("\n测试获取少量股票数据...")
    try:
        test_stocks = ['000001.SZ', '600000.SH', '000858.SZ']
        stocks_data = fetcher.get_multi_stocks_daily_k(test_stocks, start_date='20251201', end_date='20251215')
        print(f"成功获取{len(stocks_data)}只股票的数据")
        for ts_code, df in stocks_data.items():
            print(f"{ts_code}: {len(df)}行数据")
    except Exception as e:
        print(f"批量获取数据失败：{e}")


def main():
    """
    主测试函数
    """
    print("开始测试系统功能...\n")
    
    # 测试数据获取
    test_data_fetch()
    
    # 测试指标计算
    test_indicators_calculation()
    
    # 测试批量获取
    test_batch_fetch()
    
    print("\n=== 所有测试完成 ===")


if __name__ == "__main__":
    main()