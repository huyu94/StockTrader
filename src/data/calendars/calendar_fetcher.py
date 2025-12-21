import pandas as pd
from src.data.providers.base import BaseProvider
from src.data.providers.tushare_provider import TushareProvider
from src.data.providers.akshare_provider import AkShareProvider
from src.data.calendars.calendar_cache import load as cache_load, save as cache_save

class CalendarFetcher:
    def __init__(self, provider_name: str = "tushare", provider: BaseProvider | None = None):
        if provider is not None:
            self.provider: BaseProvider = provider
        else:
            if provider_name == "akshare":
                self.provider: BaseProvider = AkShareProvider()
            else:
                self.provider: BaseProvider = TushareProvider()

    def fetch(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.provider.get_trade_calendar(start_date=start_date, end_date=end_date)
        if "cal_date" in df.columns:
            df["cal_date"] = df["cal_date"].astype(str)
        df = df.drop_duplicates().sort_values(by="cal_date")
        return df

    def ensure(self, exchange: str, start_date: str, end_date: str) -> pd.DataFrame:
        cached = cache_load(exchange)
        if cached is None or cached.empty:
            df = self.fetch(start_date, end_date)
            cache_save(exchange, df)
            return df
        if "cal_date" in cached.columns:
            cached["cal_date"] = cached["cal_date"].astype(str)
        cached_min = str(cached["cal_date"].min())
        cached_max = str(cached["cal_date"].max())
        need_parts = []
        if start_date < cached_min:
            need_parts.append((start_date, cached_min))
        if end_date > cached_max:
            need_parts.append((cached_max, end_date))
        if need_parts:
            parts = [self.fetch(s, e) for s, e in need_parts if s and e and s <= e]
            if parts:
                add_df = pd.concat(parts, ignore_index=True)
                merged = pd.concat([cached, add_df], ignore_index=True)
                merged = merged.drop_duplicates().sort_values(by="cal_date")
                cache_save(exchange, merged)
                return merged
        return cached
