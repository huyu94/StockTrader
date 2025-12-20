import os
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple, List
from project_var import DATA_DIR
from src.data.calendars.calendar_cache import load as cache_load
from src.data.calendars.calendar_fetcher import CalendarFetcher

@dataclass
class MissingInfo:
    is_need_fetch: bool
    missing_dates: List[str]
    local_coverage: Tuple[str, str]
    reason: str

class StockLoader:
    def __init__(self, provider_name: str = "tushare"):
        self.calendar_fetcher = CalendarFetcher(provider_name=provider_name)

    def load(self, ts_code: str) -> pd.DataFrame | None:
        path = os.path.join(DATA_DIR, "stock_data", f"{ts_code}.csv")
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path, encoding="utf-8-sig")
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df

    def _ensure_calendar(self, exchange: str, start_date: str, end_date: str) -> pd.DataFrame:
        cal = cache_load(exchange)
        if cal is None or cal.empty:
            return self.calendar_fetcher.ensure(exchange, start_date, end_date)
        cached_min = str(cal["cal_date"].min()) if "cal_date" in cal.columns and not cal.empty else None
        cached_max = str(cal["cal_date"].max()) if "cal_date" in cal.columns and not cal.empty else None
        if cached_min is None or cached_max is None or start_date < cached_min or end_date > cached_max:
            return self.calendar_fetcher.ensure(exchange, start_date, end_date)
        return cal

    def check_missing(self, ts_code: str, start_date: str = None, end_date: str = None, exchange: str = "SSE") -> MissingInfo:
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - pd.Timedelta(days=365)).strftime("%Y%m%d")
        df_local = self.load(ts_code)
        if df_local is None or df_local.empty:
            cal = self._ensure_calendar(exchange, start_date, end_date)
            cal["cal_date"] = pd.to_datetime(cal["cal_date"], format="%Y%m%d")
            trade_dates = pd.DatetimeIndex(cal[cal["is_open"] == 1]["cal_date"].tolist())
            return MissingInfo(True, [d.strftime("%Y%m%d") for d in trade_dates], ("", ""), "no_local_data")
        cal = self._ensure_calendar(exchange, start_date, end_date)
        cal["cal_date"] = pd.to_datetime(cal["cal_date"], format="%Y%m%d")
        trade_dates = pd.DatetimeIndex(cal[cal["is_open"] == 1]["cal_date"].tolist())
        df_range = df_local[(df_local["trade_date"] >= pd.to_datetime(start_date)) & (df_local["trade_date"] <= pd.to_datetime(end_date))]
        existing = pd.DatetimeIndex(df_range["trade_date"].tolist()) if not df_range.empty else pd.DatetimeIndex([])
        missing = trade_dates.difference(existing)
        missing = missing.sort_values()
        local_min = df_local["trade_date"].min().strftime("%Y%m%d") if "trade_date" in df_local.columns and not df_local.empty else ""
        local_max = df_local["trade_date"].max().strftime("%Y%m%d") if "trade_date" in df_local.columns and not df_local.empty else ""
        return MissingInfo(len(missing) > 0, [d.strftime("%Y%m%d") for d in missing], (local_min, local_max), "missing_trading_days" if len(missing) > 0 else "ok")

    def check_missing_multi(self, ts_codes: list, start_date: str = None, end_date: str = None, exchange: str = "SSE", max_workers: int = 16) -> pd.DataFrame:
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - pd.Timedelta(days=365)).strftime("%Y%m%d")
        cal = self._ensure_calendar(exchange, start_date, end_date)
        cal["cal_date"] = pd.to_datetime(cal["cal_date"], format="%Y%m%d")
        trade_dates = pd.DatetimeIndex(cal[cal["is_open"] == 1]["cal_date"].tolist())
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = []
        def work(code: str):
            df_local = self.load(code)
            if df_local is None or df_local.empty or "trade_date" not in df_local.columns:
                return {"ts_code": code, "missing_count": len(trade_dates), "reason": "no_local_data"}
            df_range = df_local[(df_local["trade_date"] >= pd.to_datetime(start_date)) & (df_local["trade_date"] <= pd.to_datetime(end_date))]
            existing = pd.DatetimeIndex(df_range["trade_date"].tolist())
            miss = len(trade_dates.difference(existing))
            return {"ts_code": code, "missing_count": miss, "reason": "missing_trading_days" if miss > 0 else "ok"}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(work, c): c for c in ts_codes}
            for fut in as_completed(futures):
                try:
                    results.append(fut.result())
                except Exception:
                    results.append({"ts_code": futures[fut], "missing_count": -1, "reason": "error"})
        return pd.DataFrame(results)
