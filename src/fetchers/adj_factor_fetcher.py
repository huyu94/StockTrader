import os
import threading
from datetime import datetime
from typing import Optional
import pandas as pd
from loguru import logger
from project_var import DATA_DIR, CACHE_DIR
from src.providers import BaseProvider, TushareProvider
import dotenv
dotenv.load_dotenv()

class AdjFactorFetcher:
    def __init__(self, provider_name: str = "tushare", provider: Optional[BaseProvider] = None):
        self.provider_name = provider_name
        self.provider = provider or (TushareProvider() if provider_name == "tushare" else None)
        
        # 数据目录
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        self.factor_dir = os.path.join(self.data_dir, "adj_factor")
        os.makedirs(self.factor_dir, exist_ok=True)
        
        # 缓存目录
        self.cache_dir = CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, "adj_factor_cache.csv")
        
        # 线程锁，用于保护缓存文件写入
        self._cache_lock = threading.Lock()

    def _update_cache(self, ts_code: str, updated_at: str):
        """更新缓存CSV中的记录 (线程安全)"""
        with self._cache_lock:
            try:
                if os.path.exists(self.cache_file):
                    cache_df = pd.read_csv(self.cache_file, dtype=str)
                else:
                    cache_df = pd.DataFrame(columns=["ts_code", "last_updated_at"])
                
                # 检查是否存在，存在则更新，不存在则追加
                if (cache_df["ts_code"] == ts_code).any():
                    cache_df.loc[cache_df["ts_code"] == ts_code, "last_updated_at"] = updated_at
                else:
                    new_row = pd.DataFrame({"ts_code": [ts_code], "last_updated_at": [updated_at]})
                    cache_df = pd.concat([cache_df, new_row], ignore_index=True)
                
                cache_df.to_csv(self.cache_file, index=False, encoding="utf-8")
                logger.debug(f"Cache updated for {ts_code}: {updated_at}")
            except Exception as e:
                logger.error(f"Failed to update cache for {ts_code}: {e}")

    def fetch_one(self, ts_code: str, start_date: str, end_date: str, save_local: bool = True) -> pd.DataFrame:
        df = self.provider.query("adj_factor", ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None or df.empty:
            logger.warning(f"No adj factor data found for {ts_code} in {start_date}-{end_date}")
            return df
            
        df = df.sort_values("trade_date").reset_index(drop=True)
        
        if save_local:
            path = os.path.join(self.factor_dir, f"{ts_code}.csv")
            # 数据文件是按股票分文件的，理论上不同股票不会冲突，但如果是同一股票多线程写入会有问题（但目前设计是按code分任务，不会对同一code并发）
            if os.path.exists(path):
                old = pd.read_csv(path, dtype=str)
                merged = pd.concat([old, df], ignore_index=True).drop_duplicates(subset=["trade_date"], keep="last")
                merged = merged.sort_values("trade_date").reset_index(drop=True)
                merged.to_csv(path, index=False, encoding="utf-8")
            else:
                df.to_csv(path, index=False, encoding="utf-8")
            # logger.info(f"{ts_code} 复权因子已保存到 {path}")
            
            # 更新缓存
            today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._update_cache(ts_code, today)
            
        return df
