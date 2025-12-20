import sys
import os
import time

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

def verify_batch_fetch():
    print("=== 开始验证批量获取功能 ===")
    
    # 1. 初始化 Fetcher (使用 AkShare)
    print("\n1. 初始化 Fetcher (AkShare)...")
    fetcher = StockDailyKLineFetcher(provider_name="akshare")
    
    # 2. 获取股票列表 (如果本地没有，这步会触发 API 调用)
    print("\n2. 确保获取基本信息...")
    fetcher.get_stock_basic_info(save_local=True)
    
    # 3. 验证批量获取 (限制为 5 只股票)
    limit = 5
    print(f"\n3. 验证批量获取 (限制前 {limit} 只股票)...")
    print("注意观察下方是否有进度条显示：")
    
    start_time = time.time()
    fetcher.fetch_all_stocks_last_year(limit=limit)
    end_time = time.time()
    
    print(f"\n批量获取完成，耗时: {end_time - start_time:.2f} 秒")
    
    # 4. 验证数据是否保存
    print("\n4. 验证数据保存...")
    stock_codes = fetcher.get_all_stock_codes()[:limit]
    success_count = 0
    for code in stock_codes:
        df = fetcher.load_local_data(code)
        if df is not None and not df.empty:
            print(f"  [√] {code}: 获取成功，共 {len(df)} 条记录")
            success_count += 1
        else:
            print(f"  [x] {code}: 获取失败或无数据")
            
    print(f"\n验证结果: {success_count}/{limit} 成功")

if __name__ == "__main__":
    verify_batch_fetch()
