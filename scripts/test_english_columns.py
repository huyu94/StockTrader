import sys
import os
import pandas as pd
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDataFetcher

# 测试数据是否使用英文列名保存，以及是否创建了映射文件
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 测试获取单只股票数据
    print("=== 测试获取单只股票数据 ===")
    df = fetcher.get_daily_k_data(ts_code="000001.SZ", start_date="20251201", end_date="20251210", save_local=True)
    
    if df is not None:
        print(f"成功获取数据，数据条数：{len(df)}")
        print("数据列名：")
        print(df.columns.tolist())
        print("\n数据前5行：")
        print(df.head())
    
    # 检查是否创建了映射文件
    print("\n=== 检查中英文列名映射文件 ===")
    mapping_file = os.path.join("data", "column_mapping.csv")
    if os.path.exists(mapping_file):
        print(f"映射文件已创建：{mapping_file}")
        # 读取映射文件内容
        import pandas as pd
        mapping_df = pd.read_csv(mapping_file)
        print("映射文件内容：")
        print(mapping_df)
    else:
        print("映射文件未创建")
    
    # 检查生成的股票数据文件
    print("\n=== 检查生成的股票数据文件 ===")
    stock_file = os.path.join("data", "stock_data", "000001.SZ.csv")
    if os.path.exists(stock_file):
        print(f"股票数据文件已创建：{stock_file}")
        # 读取股票数据文件内容
        stock_df = pd.read_csv(stock_file)
        print("股票数据文件列名：")
        print(stock_df.columns.tolist())
        print("\n股票数据文件前5行：")
        print(stock_df.head())
    else:
        print("股票数据文件未创建")
    
    print("\n=== 测试完成 ===")