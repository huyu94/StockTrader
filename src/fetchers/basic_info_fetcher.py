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

class BasicInfoFetcher:
    """股票基本信息获取器
    1. cache： 缓存股票列表最新更新日期
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

    def _get_cache_path(self) -> str:
        return os.path.join(self.cache_dir, "basic_info_update.json")

    def _load_cache(self) -> Dict:
        cache_path = self._get_cache_path()
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cache for basic info: {e}")
        return {}

    def _save_cache(self, updated_at: str):
        cache_path = self._get_cache_path()
        data = {
            "last_updated_at": updated_at
        }
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug(f"Cache updated for basic info: {data}")
        except Exception as e:
            logger.error(f"Failed to save cache for basic info: {e}")

    def fetch(self, list_status: str = "L", 
                    exchange: str = None,
                    market: str = None,
                    is_hs: str = None,
                    fields: Optional[str] = None, 
                    filename: Optional[str] = None, 
                    save_local: bool = True, 
                    ) -> pd.DataFrame:

        """
        获取股票基本信息，并更新缓存信息
        exchange: 交易所 SSE上交所 SZSE深交所 BSE北交所
        market: 市场类别 （主板/创业板/科创板/CDR/北交所）
        is_hs: 是否沪深港通标的，N否 H沪股通 S深股通

        """
        if self.provider_name != "tushare":
            raise ValueError("Only tushare provider is supported for basic info fetching")
        
        # 默认字段
        if not fields:
            fields = "ts_code,symbol,name,area,industry,market,list_date,list_status,is_hs,exchange"
        
        params = {"list_status": list_status}
        if exchange:
            params["exchange"] = exchange
        if market:
            params["market"] = market
        if is_hs:
            params["is_hs"] = is_hs
            
        logger.info(f"Fetching basic info (list_status={list_status}, exchange={exchange}, market={market}, is_hs={is_hs})...")
        df = self.provider.query("stock_basic", fields=fields, **params)
        
        if save_local:
            name = filename or "basic_info.csv"
            path = os.path.join(self.data_dir, name)
            df.to_csv(path, index=False, encoding="utf-8")
            logger.info(f"股票基本信息已保存到 {path}")
            
            # 更新缓存
            today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_cache(today)
            
        return df

    def save_local(self, df: pd.DataFrame, filename: Optional[str] = None):
        name = filename or "basic_info.csv"
        path = os.path.join(self.data_dir, name)
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"股票基本信息已保存到 {path}")
