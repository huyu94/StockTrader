#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票分析系统示例脚本
展示如何使用各个模块的功能
"""

import sys
import os
import pandas as pd

# 将项目根目录添加到Python搜索路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 注意：在运行此脚本前，请先在src/config.py中配置您的Tushare API密钥

# 示例1：获取股票数据
print("=== 示例1：获取股票数据 ===")
try:
    from src.data_fetch.stock_data_fetcher import StockDataFetcher
    
    # 初始化数据获取器
    fetcher = StockDataFetcher()
    
    print("1. 注意：要运行数据获取示例，请先在src/config.py中配置您的Tushare API密钥")
    print("2. 配置完成后，您可以取消以下代码的注释来测试数据获取功能：")
    print("   # df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')")
    print("   # print(f'成功获取000001.SZ的数据，共{len(df)}行')")
    print("   # print(df.head())")
    
except Exception as e:
    print(f"数据获取示例错误：{e}")

# 示例2：技术指标计算
print("\n=== 示例2：技术指标计算 ===")

# 创建模拟数据用于测试
simulated_data = {
    'trade_date': pd.date_range(start='2024-01-01', periods=30),
    'open': [10.0, 10.2, 10.3, 10.5, 10.4, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.4, 11.3, 11.2, 11.1, 11.0, 10.9, 10.8, 10.7, 10.6, 10.5, 10.4, 10.3, 10.2, 10.1, 10.0],
    'high': [10.2, 10.3, 10.4, 10.6, 10.5, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.5, 11.4, 11.3, 11.2, 11.1, 11.0, 10.9, 10.8, 10.7, 10.6, 10.5, 10.4, 10.3, 10.2, 10.1],
    'low': [9.9, 10.1, 10.2, 10.4, 10.3, 10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.3, 11.2, 11.1, 11.0, 10.9, 10.8, 10.7, 10.6, 10.5, 10.4, 10.3, 10.2, 10.1, 10.0, 9.9],
    'close': [10.1, 10.2, 10.3, 10.5, 10.4, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.4, 11.3, 11.2, 11.1, 11.0, 10.9, 10.8, 10.7, 10.6, 10.5, 10.4, 10.3, 10.2, 10.1, 10.0],
    'vol': [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 14000, 13000, 12000, 11000, 10000, 9000, 8000, 7000, 6000, 5000, 4000, 3000, 2000, 1000, 500]
}

df = pd.DataFrame(simulated_data)
print(f"1. 创建了模拟股票数据，共{len(df)}行")
print("   数据示例：")
print(df.head())

try:
    from src.indicators.technical_indicators import TechnicalIndicators
    
    # 初始化指标计算器
    calculator = TechnicalIndicators()
    
    # 计算所有指标
    df_with_indicators = calculator.calculate_all_indicators(df)
    print("\n2. 成功计算所有技术指标")
    print(f"   包含的列：{list(df_with_indicators.columns)}")
    print("   技术指标示例：")
    indicator_cols = ['trade_date', 'BBI', 'MACD', 'DEA', 'MACD_HIST', 'RSI', 'K', 'D', 'J']
    print(df_with_indicators[indicator_cols].tail())
    
except Exception as e:
    print(f"技术指标计算示例错误：{e}")

# 示例3：策略筛选
print("\n=== 示例3：策略筛选 ===")
try:
    from src.strategies.base_strategy import BBIStrategy, MACDGoldenCrossStrategy
    
    # 使用模拟数据创建多只股票的数据
    stocks_data = {
        '000001.SZ': df_with_indicators.copy(),
        '600000.SH': df_with_indicators.copy(),
        '000858.SZ': df_with_indicators.copy()
    }
    
    print("1. 初始化BBI策略")
    bbi_strategy = BBIStrategy()
    
    # 筛选股票
    selected_stocks = bbi_strategy.filter_stocks(stocks_data)
    print(f"2. 使用BBI策略筛选结果：{selected_stocks}")
    
    # 初始化MACD策略
    macd_strategy = MACDGoldenCrossStrategy()
    
    # 筛选股票
    selected_stocks = macd_strategy.filter_stocks(stocks_data)
    print(f"3. 使用MACD策略筛选结果：{selected_stocks}")
    
except Exception as e:
    print(f"策略筛选示例错误：{e}")

# 示例4：可视化
print("\n=== 示例4：数据可视化 ===")
try:
    from src.visualization.stock_visualizer import StockVisualizer
    
    print("1. 初始化可视化工具")
    visualizer = StockVisualizer()
    
    print("2. 注意：可视化示例需要实际数据，您可以取消以下代码的注释来测试：")
    print("   # visualizer.plot_kline_with_bbi(df_with_indicators, '000001.SZ', save=True)")
    print("   # visualizer.plot_rsi(df_with_indicators, '000001.SZ', save=True)")
    print("   # visualizer.plot_all_indicators(df_with_indicators, '000001.SZ', save=True)")
    
except Exception as e:
    print(f"可视化示例错误：{e}")

# 示例5：启动MCP服务器
print("\n=== 示例5：启动MCP服务器 ===")
print("1. 要启动MCP服务器，请运行以下命令：")
print("   uv run python src/server/mcp_server.py")
print("2. 服务器启动后，您可以通过以下地址访问API文档：")
print("   http://localhost:8000/docs")
print("3. 您可以使用API测试工具（如Postman或curl）来测试各个API端点")

print("\n=== 示例脚本执行完成 ===")
print("要使用完整功能，请确保：")
print("1. 在src/config.py中配置了有效的Tushare API密钥")
print("2. 安装了所有依赖库（已通过uv add命令安装）")
print("3. 了解各个模块的使用方法")
