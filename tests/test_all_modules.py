#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本，用于测试各个模块的功能
"""

import sys
import os

# 将项目根目录添加到Python搜索路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.indicators.technical_indicators import TechnicalIndicators
from src.strategies.base_strategy import (
    BBIStrategy, MACDGoldenCrossStrategy,
    RSIStrategy, KDJStrategy, CombinedStrategy
)
from src.visualization.stock_visualizer import StockVisualizer


def test_data_fetch():
    """
    测试数据获取模块
    """
    print("=== 测试数据获取模块 ===")
    
    fetcher = StockDailyKLineFetcher()
    
    # 测试获取单只股票数据
    print("\n1. 测试获取单只股票数据...")
    try:
        df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
        print(f"   成功获取000001.SZ的数据，共{len(df)}行")
        print(f"   数据示例：")
        print(df.head())
        return df
    except Exception as e:
        print(f"   获取数据失败：{e}")
        return None


def test_indicators_calculation(df):
    """
    测试技术指标计算模块
    """
    if df is None:
        print("\n=== 跳过技术指标测试（数据获取失败） ===")
        return None
    
    print("\n=== 测试技术指标计算模块 ===")
    
    calculator = TechnicalIndicators()
    
    # 测试计算所有指标
    print("\n1. 测试计算所有指标...")
    try:
        df_with_indicators = calculator.calculate_all_indicators(df)
        print(f"   成功计算所有指标")
        print(f"   包含的列：{list(df_with_indicators.columns)}")
        print(f"   数据示例（只显示技术指标列）：")
        indicator_cols = ['BBI', 'MACD', 'DEA', 'MACD_HIST', 'RSI', 'K', 'D', 'J']
        print(df_with_indicators[indicator_cols].tail())
        return df_with_indicators
    except Exception as e:
        print(f"   计算指标失败：{e}")
        return None


def test_strategy_filtering():
    """
    测试策略筛选模块
    """
    print("\n=== 测试策略筛选模块 ===")
    
    fetcher = StockDailyKLineFetcher()
    
    # 获取少量股票数据用于测试
    test_stocks = ['000001.SZ', '600000.SH', '000858.SZ', '000002.SZ', '600036.SH']
    
    print(f"\n1. 获取测试股票数据：{test_stocks}")
    try:
        stocks_data = fetcher.get_multi_stocks_daily_k(
            test_stocks, 
            start_date='20240101', 
            end_date='20241231'
        )
        print(f"   成功获取{len(stocks_data)}只股票的数据")
    except Exception as e:
        print(f"   获取测试数据失败：{e}")
        return
    
    # 测试各个策略
    strategies = [
        ("BBI策略", BBIStrategy()),
        ("MACD策略", MACDGoldenCrossStrategy()),
        ("RSI策略", RSIStrategy()),
        ("KDJ策略", KDJStrategy()),
        ("组合策略", CombinedStrategy())
    ]
    
    for strategy_name, strategy in strategies:
        print(f"\n2. 测试{strategy_name}...")
        try:
            selected_stocks = strategy.filter_stocks(stocks_data)
            print(f"   符合{strategy_name}的股票：{selected_stocks}")
        except Exception as e:
            print(f"   {strategy_name}测试失败：{e}")


def test_visualization(df_with_indicators):
    """
    测试可视化模块
    """
    if df_with_indicators is None:
        print("\n=== 跳过可视化测试（数据获取或指标计算失败） ===")
        return
    
    print("\n=== 测试可视化模块 ===")
    
    visualizer = StockVisualizer()
    
    # 测试绘制K线图和BBI
    print("\n1. 测试绘制K线图和BBI...")
    try:
        # 只保存图片，不显示（避免阻塞）
        visualizer.plot_kline_with_bbi(df_with_indicators, '000001.SZ', save=True)
        print("   成功绘制K线图和BBI")
    except Exception as e:
        print(f"   绘制失败：{e}")
    
    # 测试绘制RSI
    print("\n2. 测试绘制RSI...")
    try:
        visualizer.plot_rsi(df_with_indicators, '000001.SZ', save=True)
        print("   成功绘制RSI指标")
    except Exception as e:
        print(f"   绘制失败：{e}")
    
    print("\n可视化测试完成，图表已保存到visualizations目录")


def main():
    """
    主测试函数
    """
    print("开始测试股票分析系统各个模块...\n")
    
    # 1. 测试数据获取
    df = test_data_fetch()
    
    # 2. 测试技术指标计算
    df_with_indicators = test_indicators_calculation(df)
    
    # 3. 测试策略筛选
    test_strategy_filtering()
    
    # 4. 测试可视化
    test_visualization(df_with_indicators)
    
    print("\n=== 所有测试完成 ===")


if __name__ == "__main__":
    main()