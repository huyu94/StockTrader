from src.data_fetch.stock_data_fetcher import StockDataFetcher

# 测试fetch功能
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 获取单只股票数据，测试是否能正确保存原始股价和复权因子
    df = fetcher.get_daily_k_data('000001.SZ', start_date='20251201', end_date='20251215', save_local=True)
    print("测试完成，查看data目录下是否生成了对应的文件")