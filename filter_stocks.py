#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票筛选脚本
用于筛选2025年12月15日收盘后符合以下条件的股票：
1. 热门板块（科技、核聚变、商业航天等）
2. 前20日内最低点到最高点，涨幅大于20%
3. 最后一天收盘，KDJ指标小于10
"""

import sys
import os

# 将项目根目录添加到Python搜索路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.data_fetch.stock_data_fetcher import StockDataFetcher
from src.strategies.base_strategy import HotPlateStrategy


def filter_stocks():
    """
    执行股票筛选
    """
    print("=== 股票筛选工具 ===")
    print("筛选条件：")
    print("1. 热门板块（科技、核聚变、商业航天等）")
    print("2. 前20日内最低点到最高点，涨幅大于20%")
    print("3. 最后一天收盘，KDJ指标小于10")
    print("\n注意：使用前请确保已在src/config.py中配置了有效的Tushare API密钥")
    
    # 检查配置文件
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "config.py")
    if not os.path.exists(config_path):
        print(f"\n错误：配置文件不存在，请创建{config_path}文件并配置Tushare API密钥")
        return
    
    with open(config_path, "r", encoding="utf-8") as f:
        config_content = f.read()
        if "your_tushare_token_here" in config_content:
            print(f"\n提示：请先在{config_path}中配置您的Tushare API密钥")
            print("示例：TUSHARE_TOKEN = \"your_actual_token_here\"")
            return
    
    print("\n1. 初始化数据获取器...")
    fetcher = StockDataFetcher()
    
    print("\n2. 初始化热门板块策略...")
    strategy = HotPlateStrategy()
    
    print("\n3. 加载所有股票基本信息...")
    if strategy.stock_basic.empty:
        print("   加载股票基本信息失败，无法进行筛选")
        return
    
    print(f"   成功加载{len(strategy.stock_basic)}只股票的基本信息")
    
    # 获取所有热门板块的股票
    print("\n4. 筛选热门板块股票...")
    hot_stocks = []
    for _, stock in strategy.stock_basic.iterrows():
        if strategy.is_hot_industry(stock['ts_code']):
            hot_stocks.append(stock['ts_code'])
    
    print(f"   共筛选出{len(hot_stocks)}只热门板块股票")
    print(f"   示例：{hot_stocks[:5]}...")
    
    # 提示用户是否继续
    print(f"\n5. 注意：接下来将获取{len(hot_stocks)}只股票的历史数据，这可能需要较长时间")
    answer = input("   是否继续？(y/n): ")
    if answer.lower() != 'y':
        print("   取消筛选")
        return
    
    # 获取股票数据
    print("\n6. 获取股票历史数据...")
    start_date = '20251101'  # 前20天的数据
    end_date = '20251215'    # 截止到2025年12月15日
    
    print(f"   时间范围：{start_date} 到 {end_date}")
    print("   正在获取数据，请稍候...")
    
    # 分批获取数据，每批100只股票
    batch_size = 100
    stocks_data = {}
    
    for i in range(0, len(hot_stocks), batch_size):
        batch_stocks = hot_stocks[i:i+batch_size]
        print(f"   正在获取第{i//batch_size+1}批数据，共{len(batch_stocks)}只股票")
        
        try:
            batch_data = fetcher.get_multi_stocks_daily_k(batch_stocks, start_date, end_date)
            stocks_data.update(batch_data)
        except Exception as e:
            print(f"   获取第{i//batch_size+1}批数据失败：{e}")
            continue
    
    print(f"\n7. 成功获取{len(stocks_data)}只股票的数据")
    
    # 使用策略筛选
    print("\n8. 使用热门板块策略筛选股票...")
    selected_stocks = strategy.filter_stocks(stocks_data)
    
    # 输出结果
    print("\n=== 筛选结果 ===")
    if selected_stocks:
        print(f"共筛选出{len(selected_stocks)}只符合条件的股票：")
        for i, ts_code in enumerate(selected_stocks, 1):
            # 获取股票名称
            stock_info = strategy.stock_basic[strategy.stock_basic['ts_code'] == ts_code]
            stock_name = stock_info['name'].iloc[0] if not stock_info.empty else "未知"
            print(f"{i}. {ts_code} - {stock_name}")
    else:
        print("没有找到符合条件的股票")
    
    print("\n=== 筛选完成 ===")


if __name__ == "__main__":
    filter_stocks()