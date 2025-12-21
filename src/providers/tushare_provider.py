import os
from typing import Optional, Any
import tushare as ts
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from .base_provider import BaseProvider

load_dotenv()

class TushareProvider(BaseProvider):
    """
    Tushare data provider implementation.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TushareProvider, cls).__new__(cls)
            cls._instance._init_api()
        return cls._instance

    def _init_api(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            logger.error("TUSHARE_TOKEN not found in environment variables.")
            raise ValueError("TUSHARE_TOKEN not found. Please set it in .env file.")
        
        # Initialize Tushare Pro API
        self.pro = ts.pro_api(token)
        logger.info("Tushare API initialized.")

    def query(self, api_name: str, fields: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
        """
        Execute a query against Tushare API.
        """
        try:
            df = self.pro.query(api_name, fields=fields, **kwargs)
            return df
        except Exception as e:
            logger.error(f"Tushare query failed: {e}")
            raise
