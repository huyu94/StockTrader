import os, sys
project_path = os.path.dirname(os.path.dirname( os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# print(project_path)
sys.path.append(project_path)

from src.fetch.providers.tushare_provider import TushareProvider
from src.storage.adj_factor_storage_mysql import AdjFactorStorageMySQL
from src.fetch.adj_factor.adj_factor_fetcher import AdjFactorFetcher

def test_adj_factor_fetch():
    provider = TushareProvider()
    df = provider.pro.query('adj_factor', ts_code='000001.SZ, 000002.SZ, 000003.SZ', trade_date='20251222')
    print(df)

def main():
    
    provider = TushareProvider()
    storage = AdjFactorStorageMySQL()
    adj_factor = AdjFactorFetcher(provider, storage)

    adj_factor.update("2025-01-01", "2025-12-25")

if __name__ == "__main__":
    # test_adj_factor_fetch()
    main()