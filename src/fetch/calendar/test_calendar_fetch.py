import os, sys
project_path = os.path.dirname(os.path.dirname( os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# print(project_path)
sys.path.append(project_path)


from calendar_fetcher import CalendarFetcher

from src.fetch.providers.tushare_provider import TushareProvider
from src.storage.calendar_storage_mysql import CalendarStorageMySQL



if __name__ == "__main__":
    calendar_fetcher = CalendarFetcher(TushareProvider(), CalendarStorageMySQL())
    df = calendar_fetcher.upadte(start_date="20250101", end_date="20251227", exchange='SSE')
