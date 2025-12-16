import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDataFetcher

# 测试不同参数组合下的get_batch_daily_k_data方法
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 测试1：指定单个股票代码和日期范围
    print("\n=== 测试1：指定单个股票代码和日期范围 ===")
    result1 = fetcher.get_batch_daily_k_data(ts_code="000001.SZ", start_date="20251201", end_date="20251210", save_local=False)
    print(f"成功获取{len(result1)}只股票的数据")
    if result1:
        ts_code = list(result1.keys())[0]
        print(f"{ts_code}的数据条数：{len(result1[ts_code])}")
    
    # 测试2：指定多个股票代码和日期范围
    print("\n=== 测试2：指定多个股票代码和日期范围 ===")
    result2 = fetcher.get_batch_daily_k_data(ts_code="000001.SZ,600000.SH", start_date="20251201", end_date="20251210", save_local=False)
    print(f"成功获取{len(result2)}只股票的数据")
    for ts_code, df in result2.items():
        print(f"{ts_code}的数据条数：{len(df)}")
    
    # 测试3：指定单个交易日期
    print("\n=== 测试3：指定单个交易日期 ===")
    result3 = fetcher.get_batch_daily_k_data(trade_date="20251215", save_local=False)
    print(f"成功获取{len(result3)}只股票的数据")
    if result3:
        # 随机选择几只股票显示数据条数
        sample_codes = list(result3.keys())[:3]
        for ts_code in sample_codes:
            print(f"{ts_code}的数据条数：{len(result3[ts_code])}")
    
    # 测试4：指定股票代码和单个交易日期
    print("\n=== 测试4：指定股票代码和单个交易日期 ===")
    result4 = fetcher.get_batch_daily_k_data(ts_code="000001.SZ,600000.SH", trade_date="20251215", save_local=False)
    print(f"成功获取{len(result4)}只股票的数据")
    for ts_code, df in result4.items():
        print(f"{ts_code}的数据条数：{len(df)}")
    
    print("\n=== 所有测试完成 ===")