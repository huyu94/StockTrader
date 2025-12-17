import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

# 测试get_daily_k_data方法是否包含所有pro.daily接口的输出字段
if __name__ == "__main__":
    fetcher = StockDailyKLineFetcher()
    
    print("=== 测试get_daily_k_data方法 ===")
    # 获取单只股票数据
    df = fetcher.get_daily_k_data('000001.SZ', start_date='20251201', end_date='20251210', save_local=False)
    
    if df is not None:
        print(f"成功获取000001.SZ的数据，共{len(df)}行")
        print("数据列名：")
        print(list(df.columns))
        print("\n数据样例（前5行）：")
        print(df.head())
    
    print("\n=== 测试get_batch_daily_k_data方法 ===")
    # 测试指定多个股票代码和日期范围
    result = fetcher.get_batch_daily_k_data(ts_code="000001.SZ,600000.SH", start_date="20251201", end_date="20251210", save_local=False)
    
    if result:
        print(f"成功获取{len(result)}只股票的数据")
        for ts_code, df in result.items():
            print(f"\n{ts_code}的数据列名：")
            print(list(df.columns))