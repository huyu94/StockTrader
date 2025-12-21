import os
from typing import Optional, List, Iterable
import pandas as pd
from loguru import logger
from project_var import DATA_DIR
from src.providers import BaseProvider, TushareProvider
import dotenv
dotenv.load_dotenv()

class DailyKlineFetcher:
    def __init__(self, provider_name: str = "tushare", provider: Optional[BaseProvider] = None):
        self.provider_name = provider_name
        self.provider = provider or (TushareProvider() if provider_name == "tushare" else None)
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        self.code_dir = os.path.join(self.data_dir, "daily")
        self.date_dir = os.path.join(self.data_dir, "daily_by_date")
        os.makedirs(self.code_dir, exist_ok=True)
        os.makedirs(self.date_dir, exist_ok=True)

    def fetch_one(self, ts_code: str, start_date: str, end_date: str, save_local: bool = True) -> pd.DataFrame:
        df = self.provider.query("daily", ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return df
        df = df.sort_values("trade_date").reset_index(drop=True)
        if save_local:
            path = os.path.join(self.code_dir, f"{ts_code}.csv")
            if os.path.exists(path):
                old = pd.read_csv(path, dtype=str)
                merged = pd.concat([old, df], ignore_index=True).drop_duplicates(subset=["trade_date"], keep="last")
                merged = merged.sort_values("trade_date").reset_index(drop=True)
                merged.to_csv(path, index=False, encoding="utf-8")
            else:
                df.to_csv(path, index=False, encoding="utf-8")
            logger.info(f"{ts_code} 日K线已保存到 {path}")
        return df

    def fetch_by_date(self, trade_date: str, codes: Optional[Iterable[str]] = None, save_local: bool = True) -> List[str]:
        if codes:
            written = []
            for c in codes:
                out = self.fetch_one(ts_code=c, start_date=trade_date, end_date=trade_date, save_local=save_local)
                if out is not None and not out.empty:
                    written.append(c)
            if save_local:
                return written
            return written
        df = self.provider.query("daily", trade_date=trade_date)
        if df is None or df.empty:
            return []
        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
        if save_local:
            path = os.path.join(self.date_dir, f"{trade_date}.csv")
            df.to_csv(path, index=False, encoding="utf-8")
            logger.info(f"{trade_date} 全市场日K线已保存到 {path}")
        return list(df["ts_code"].unique())
