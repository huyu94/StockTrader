import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher




if __name__ == "__main__":
    fetcher = StockDailyKLineFetcher()
    print('获取股票基本信息')
    fetcher.get_stock_basic_info()