#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票筛选Demo脚本
筛选条件：
1. 前20日内最低点到最高点，涨幅大于20%
2. 最后一天收盘，KDJ指标小于10
"""

import os
import sys
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

# -------------------------- 配置 --------------------------
# 从环境变量获取Tushare API密钥
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "your_tushare_token_here")

# 筛选日期范围
END_DATE = "20251215"  # 最后一天
START_DATE = "20251101"  # 前20天

# -------------------------- 初始化 --------------------------
# 设置Tushare API密钥
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

print("=== 股票筛选Demo ===")
print(f"筛选日期范围：{START_DATE} 到 {END_DATE}")
print(f"筛选条件：")
print(f"1. 前20日内最低点到最高点，涨幅大于20%")
print(f"2. 最后一天收盘，KDJ指标小于10\n")

# -------------------------- 函数定义 --------------------------
def get_stock_basic():
    """
    获取所有A股股票的基本信息
    """
    print("1. 获取所有A股股票基本信息...")
    try:
        # 获取所有上市股票
        stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code, symbol, name, industry')
        print(f"   成功获取{len(stock_basic)}只股票的基本信息")
        return stock_basic
    except Exception as e:
        print(f"   获取股票基本信息失败：{e}")
        sys.exit(1)

def get_daily_data(ts_code):
    """
    获取单只股票的日线数据
    """
    try:
        df = pro.daily(ts_code=ts_code, start_date=START_DATE, end_date=END_DATE)
        if df.empty:
            return None
        
        # 转换日期格式并排序
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values('trade_date')
        
        # 重命名列名以便后续计算
        df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
        return df
    except Exception as e:
        return None

def calculate_kdj(df, n=9, m1=3, m2=3):
    """
    计算KDJ指标
    """
    df_copy = df.copy()
    
    # 计算RSV值
    df_copy['low_n'] = df_copy['low'].rolling(window=n).min()
    df_copy['high_n'] = df_copy['high'].rolling(window=n).max()
    df_copy['RSV'] = (df_copy['close'] - df_copy['low_n']) / (df_copy['high_n'] - df_copy['low_n']) * 100
    
    # 计算K、D、J值
    df_copy['K'] = df_copy['RSV'].ewm(com=m1-1, adjust=False).mean()
    df_copy['D'] = df_copy['K'].ewm(com=m2-1, adjust=False).mean()
    df_copy['J'] = 3 * df_copy['K'] - 2 * df_copy['D']
    
    return df_copy

def check_20d_gain(df):
    """
    检查前20日内最低点到最高点的涨幅是否大于20%
    """
    if len(df) < 20:
        return False
    
    # 最近20天的数据
    recent_20d = df.tail(20)
    
    low = recent_20d['low'].min()
    high = recent_20d['high'].max()
    
    # 计算涨幅
    gain = (high - low) / low * 100
    
    return gain > 20

def check_kdj_less_than_10(df):
    """
    检查最后一天的KDJ指标是否小于10
    """
    if len(df) < 20:
        return False
    
    last_day = df.iloc[-1]
    
    return last_day['K'] < 10 and last_day['D'] < 10 and last_day['J'] < 10

def filter_stocks(stock_basic):
    """
    筛选符合条件的股票
    """
    print("\n2. 开始筛选符合条件的股票...")
    print("   正在获取数据并计算指标，这可能需要一段时间，请稍候...")
    
    filtered_stocks = []
    total_stocks = len(stock_basic)
    
    # 只测试前50只股票，加快演示速度
    # 实际使用时可以注释掉这行，测试所有股票
    test_stocks = stock_basic.head(50)
    
    # 输出debug信息，显示测试的50只股票
    print("   \nDEBUG: 测试的50只股票列表：")
    print("   股票代码    股票名称    所属行业")
    print("   --------------------------------------------------")
    for i, stock in test_stocks.iterrows():
        print(f"   {stock['ts_code']}    {stock['name']}    {stock['industry']}")
        if (i + 1) % 10 == 0 and i + 1 < 50:
            print("   --------------------------------------------------")
    print("   --------------------------------------------------")
    print()
    
    for i, stock in test_stocks.iterrows():
        ts_code = stock['ts_code']
        name = stock['name']
        
        # 打印进度
        if (i + 1) % 10 == 0:
            print(f"   已处理 {i + 1}/{len(test_stocks)} 只股票")
        
        # 获取日线数据
        df = get_daily_data(ts_code)
        if df is None:
            continue
        
        # 计算KDJ指标
        df_with_kdj = calculate_kdj(df)
        
        # 检查条件1：前20日涨幅大于20%
        if not check_20d_gain(df_with_kdj):
            continue
        
        # 检查条件2：最后一天KDJ小于10
        if not check_kdj_less_than_10(df_with_kdj):
            continue
        
        # 符合条件，添加到结果列表
        filtered_stocks.append({
            'ts_code': ts_code,
            'name': name,
            'industry': stock['industry']
        })
    
    return filtered_stocks

def main():
    """
    主函数
    """
    # 1. 获取股票基本信息
    stock_basic = get_stock_basic()
    
    # 2. 筛选符合条件的股票
    filtered_stocks = filter_stocks(stock_basic)
    
    # 3. 输出结果
    print(f"\n3. 筛选结果：")
    if filtered_stocks:
        print(f"   共找到 {len(filtered_stocks)} 只符合条件的股票：")
        print("   --------------------------------------------------")
        print("   股票代码    股票名称    所属行业")
        print("   --------------------------------------------------")
        for stock in filtered_stocks:
            print(f"   {stock['ts_code']}    {stock['name']}    {stock['industry']}")
        print("   --------------------------------------------------")
    else:
        print("   未找到符合条件的股票")
    
    print("\n=== 筛选完成 ===")

# -------------------------- 执行 --------------------------
if __name__ == "__main__":
    main()