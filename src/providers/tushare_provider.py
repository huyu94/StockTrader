import os
import time
import threading
from typing import Optional, Any
import tushare as ts
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from .base_provider import BaseProvider

load_dotenv()

class TushareProvider(BaseProvider):
    """
    Tushare data provider implementation with rate limiting and singleton pattern.
    """
    _instance = None
    _lock = threading.Lock()
    
    RATE_LIMIT_PER_MINUTE = 500
    WINDOW_SECONDS = 60
    MAX_CONCURRENT_CONNECTIONS = 2
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
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
        # timeout can be adjusted if needed
        self.pro = ts.pro_api(token)
        self.request_lock = threading.Lock()
        self.window_start = time.time()
        self.request_count = 0
        self._semaphore = threading.Semaphore(self.MAX_CONCURRENT_CONNECTIONS)
        logger.info("Tushare API initialized.")

    def _wait_for_rate_limit(self):
        """
        Enforce rate limiting by counting requests per fixed window and waiting
        when the count reaches RATE_LIMIT_PER_MINUTE. Window resets every WINDOW_SECONDS.
        """
        with self.request_lock:
            now = time.time()
            elapsed = now - self.window_start
            if elapsed >= self.WINDOW_SECONDS:
                self.window_start = now
                self.request_count = 0
            if self.request_count >= self.RATE_LIMIT_PER_MINUTE:
                sleep_time = self.window_start + self.WINDOW_SECONDS - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.window_start = time.time()
                self.request_count = 0
            self.request_count += 1

    def query(self, api_name: str, fields: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
        """
        Execute a query against Tushare API with rate limiting.
        """
        self._semaphore.acquire()
        try:
            self._wait_for_rate_limit()
            logger.debug(f"Querying Tushare: {api_name}, params: {kwargs}")
            df = self.pro.query(api_name, fields=fields, **kwargs)
            return df
        except Exception as e:
            logger.error(f"Tushare query failed: {e}")
            raise
        finally:
            self._semaphore.release()
