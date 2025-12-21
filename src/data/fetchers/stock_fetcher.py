from loguru import logger
import pandas as pd
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
from project_var import DATA_DIR
from src.data.providers.base import BaseProvider
from src.data.providers.tushare_provider import TushareProvider
from src.data.providers.akshare_provider import AkShareProvider
from src.data.calendars.calendar_fetcher import CalendarFetcher

class StockDailyKLineFetcher:

    def __init__(self, provider_name: str = "tushare", provider: BaseProvider | None = None):
        self.stock_data_path = os.path.join(DATA_DIR, "stock_data")
        os.makedirs(self.stock_data_path, exist_ok=True)
        
        if provider is not None:
            self.provider: BaseProvider = provider
        else:
            if provider_name == "akshare":
                self.provider: BaseProvider = AkShareProvider()
            else:
                self.provider: BaseProvider = TushareProvider()
        self.calendar = CalendarFetcher(provider_name=provider_name)
        self._net_sema = threading.Semaphore(2)
        self._default_days = 365

    def _infer_start_date(self, ts_code: str, end_date: str) -> str:
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        if os.path.exists(file_path):
            try:
                origin_df = pd.read_csv(file_path, encoding='utf-8-sig')
                if 'trade_date' in origin_df.columns and not origin_df.empty:
                    origin_df['trade_date'] = origin_df['trade_date'].astype(str)
                    last_str = str(origin_df['trade_date'].max())
                    last_dt = datetime.strptime(last_str, '%Y%m%d')
                    start_dt = last_dt + timedelta(days=1)
                    end_dt = datetime.strptime(end_date, '%Y%m%d')
                    if start_dt > end_dt:
                        return end_date
                    return start_dt.strftime('%Y%m%d')
            except Exception:
                pass
        start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=self._default_days)
        return start_dt.strftime('%Y%m%d')

    def fetch_one(self, ts_code: str, start_date: str = None, end_date: str = None, 
                        save_local: bool = True) -> pd.DataFrame:
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = self._infer_start_date(ts_code, end_date)
        df = self.provider.get_daily_k_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return None
        if save_local:
            self.save_to_local_data(ts_code, df)
        return df

    def fetch_batch(self, ts_codes: list, start_date: str, end_date: str, save_local: bool = True, workers: int | None = None):
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if workers is None:
            workers = 2
        successes = []
        def task(code: str):
            try:
                t0 = datetime.now()
                self._net_sema.acquire()
                try:
                    s = start_date if start_date else self._infer_start_date(code, end_date)
                    df = self.provider.get_daily_k_data(ts_code=code, start_date=s, end_date=end_date)
                finally:
                    self._net_sema.release()
                t_data = (datetime.now() - t0).total_seconds()
                if df is None or df.empty:
                    return False
                if save_local:
                    t1 = datetime.now()
                    self.save_to_local_data(code, df)
                    t_save = (datetime.now() - t1).total_seconds()
                else:
                    t_save = 0.0
                logger.debug(f"{code} data={t_data:.2f}s save={t_save:.2f}s")
                return True
            except Exception as e:
                logger.error(f"{code} 拉取失败：{e}")
                return False
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(task, c): c for c in ts_codes}
            with tqdm(total=len(ts_codes), desc="批量拉取", unit="只") as pbar:
                for fut in as_completed(futures):
                    try:
                        ok = fut.result()
                        if ok:
                            successes.append(futures[fut])
                    except Exception as e:
                        logger.error(f"{futures[fut]} 任务异常：{e}")
                    finally:
                        pbar.update(1)
        return successes

    def fetch_by_date(self, trade_date: str, codes: set[str]) -> list[str]:
        try:
            df = None
            # 优先走 Provider 的按日期全量接口
            if hasattr(self.provider, "get_daily_all"):
                df = self.provider.get_daily_all(trade_date=trade_date)
            if df is None or df.empty:
                return []
            if "ts_code" not in df.columns:
                return []
            written = []
            # 仅处理缺失的代码子集
            df = df[df["ts_code"].isin(list(codes))]
            for ts_code, g in df.groupby("ts_code"):
                # 统一列子集，避免多余列带来的不一致
                cols = [c for c in ["trade_date", "open", "high", "low", "close", "vol"] if c in g.columns]
                part = g[cols].copy()
                self.save_to_local_data(ts_code, part)
                written.append(ts_code)
            return written
        except Exception as e:
            logger.error(f"{trade_date} 批量抓取失败：{e}")
            return []

    def detect_missing_dates(self, ts_code: str, end_date: str, window_days: int = 365) -> list[str]:
        try:
            # 计算近一年窗口
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            start_dt = end_dt - timedelta(days=window_days)
            start_date = start_dt.strftime("%Y%m%d")
            # 交易日集合
            cal_df = self.calendar.fetch(start_date=start_date, end_date=end_date)
            if "is_open" in cal_df.columns:
                cal_df = cal_df[cal_df["is_open"] == 1]
            trading_days = set(cal_df["cal_date"].astype(str).tolist())
            # 本地已有
            file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
            present = set()
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, usecols=["trade_date"], encoding="utf-8-sig")
                df["trade_date"] = df["trade_date"].astype(str)
                present = {d for d in df["trade_date"].tolist() if start_date <= d <= end_date}
            missing = sorted(list(trading_days - present))
            return missing
        except Exception as e:
            logger.error(f"{ts_code} 缺口检测失败：{e}")
            return []

    def save_to_local_data(self, ts_code: str, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        file_path = os.path.join(self.stock_data_path, f"{ts_code}.csv")
        if 'trade_date' in df.columns:
            df['trade_date'] = df['trade_date'].astype(str)
        if os.path.exists(file_path):
            origin_df = pd.read_csv(file_path, encoding='utf-8-sig')
            if 'trade_date' in origin_df.columns:
                origin_df['trade_date'] = origin_df['trade_date'].astype(str)
        else:
            origin_df = None
        if origin_df is None or origin_df.empty:
            merged_df = df.copy()
        else:
            merged_df = pd.concat([origin_df, df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=['trade_date'], keep='last')
        merged_df = merged_df.sort_values(by='trade_date')
        merged_df.to_csv(file_path, index=False, encoding='utf-8-sig')
