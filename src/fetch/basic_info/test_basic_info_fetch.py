import os, sys
project_path = os.path.dirname(os.path.dirname( os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# print(project_path)
sys.path.append(project_path)



from basic_info_fetcher import BasicInfoFetcher
from src.fetch.providers.tushare_provider import TushareProvider
from src.storage.basic_info_storage_mysql import BasicInfoStorageMySQL

if __name__ == "__main__":
    basic_info_fetcher = BasicInfoFetcher(TushareProvider(), BasicInfoStorageMySQL())
    basic_info_fetcher.update()
    