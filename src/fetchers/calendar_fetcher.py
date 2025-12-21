import os
import json
from datetime import datetime
from typing import Optional, Dict
import pandas as pd
from loguru import logger
from project_var import DATA_DIR, CACHE_DIR
from src.providers import BaseProvider, TushareProvider
import dotenv
dotenv.load_dotenv()

class CalendarFetcher:
    """交易日历获取器
    1. cache： 缓存交易日历最新更新日期
    """
    def __init__(self, provider_name: str = "tushare", provider: Optional[BaseProvider] = None):
        self.provider_name = provider_name
        self.provider = provider or (TushareProvider() if provider_name == "tushare" else None)
        
        # 数据目录
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 缓存目录
        self.cache_dir = CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, exchange: str) -> str:
        return os.path.join(self.cache_dir, f"calendar_{exchange}_update.json")

    def _load_cache(self, exchange: str) -> Dict:
        cache_path = self._get_cache_path(exchange)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cache for {exchange}: {e}")
        return {}

    def _save_cache(self, exchange: str, updated_at: str):
        cache_path = self._get_cache_path(exchange)
        data = {
            "exchange": exchange,
            "last_updated_at": updated_at
        }
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug(f"Cache updated for {exchange}: {data}")
        except Exception as e:
            logger.error(f"Failed to save cache for {exchange}: {e}")

    def fetch(self, start_date: Optional[str], end_date: Optional[str], exchange: str = "SSE", filename: Optional[str] = None, save_local: bool = True) -> pd.DataFrame:
        """
        获取交易日历，并更新缓存信息（如果是全量或包含今日的更新）
        """
        if self.provider_name != "tushare":
            raise ValueError("Only tushare provider is supported for calendar fetching")
        
        # 指定返回字段，不包含 pretrade_date
        fields = "exchange,cal_date,is_open"
        
        params = {"exchange": exchange, "is_open": 1}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
            
        logger.info(f"Fetching calendar for {exchange}...")
        df = self.provider.query("trade_cal", fields=fields, **params)
        df = df.sort_values("cal_date").reset_index(drop=True)
        
        if save_local:
            name = filename or f"trade_calendar_{exchange}_{start_date or 'ALL'}_{end_date or 'ALL'}.csv"
            path = os.path.join(self.data_dir, name)
            df.to_csv(path, index=False, encoding="utf-8")
            logger.info(f"交易日历已保存到 {path}")
            
            # 更新缓存
            today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_cache(exchange, today)
            
        return df

    def save_local(self, df: pd.DataFrame, filename: Optional[str] = None):
        path = os.path.join(self.data_dir, filename)
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"交易日历已保存到 {path}")
