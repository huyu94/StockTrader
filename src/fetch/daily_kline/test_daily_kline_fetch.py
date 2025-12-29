import os, sys
project_path = os.path.dirname(os.path.dirname( os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# print(project_path)
sys.path.append(project_path)

from src.fetch.providers.tushare_provider import TushareProvider
from src.fetch.daily_kline.daily_kline_fetcher import DailyKlineFetcher
from src.storage.daily_kline_storage_mysql import DailyKlineStorageMySQL
from src.storage.adj_factor_storage_mysql import AdjFactorStorageMySQL
from src.fetch.daily_kline.qfq_calculator import QFQCalculator

def main():
    
    provider = TushareProvider()
    storage = DailyKlineStorageMySQL()
    daily_kline_fetcher = DailyKlineFetcher(provider, storage, max_write_workers=15)

    daily_kline_fetcher.update(start_date="2025-01-01", end_date="2025-12-25")


def update_qfq():

    qfq_calculator = QFQCalculator(
        daily_storage=DailyKlineStorageMySQL(), 
        adj_storage=AdjFactorStorageMySQL()
    )
    # print(qfq_calculator.get_ex_stock_codes(start_date="2025-12-01", end_date="2025-12-25"))
    qfq_calculator.update_all_qfq(start_date="2025-12-01", end_date="2025-12-25", ts_codes='920978.BJ')

if __name__ == "__main__":
    # test_adj_factor_fetch()
    # main()
    update_qfq()