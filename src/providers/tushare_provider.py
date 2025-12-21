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
    
    # 限制每分钟请求数（官方限制视积分而定，这里设置较高以利用并发，依赖重试机制处理超限）
    RATE_LIMIT_PER_MINUTE = 500
    WINDOW_SECONDS = 60
    # 限制并发连接数，避免触发IP限制 (改回2以稳妥)
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
            
            # 如果即将超过限制，强制等待到窗口结束
            if self.request_count >= self.RATE_LIMIT_PER_MINUTE:
                sleep_time = self.window_start + self.WINDOW_SECONDS - now + 1 # +1 buffer
                if sleep_time > 0:
                    logger.warning(f"Rate limit reached ({self.RATE_LIMIT_PER_MINUTE}/min). Sleeping for {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                self.window_start = time.time()
                self.request_count = 0
            
            self.request_count += 1

    def query(self, api_name: str, fields: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
        """
        Execute a query against Tushare API with rate limiting and retry logic.
        """
        self._semaphore.acquire()
        max_retries = 5 # 增加重试次数
        retry_delay = 2  # 初始重试等待减少
        
        try:
            for attempt in range(max_retries):
                try:
                    self._wait_for_rate_limit()
                    logger.debug(f"Querying Tushare: {api_name}, params: {kwargs} (Attempt {attempt+1})")
                    df = self.pro.query(api_name, fields=fields, **kwargs)
                    return df
                except Exception as e:
                    error_msg = str(e)
                    # 检查是否为 IP 限制错误或网络错误
                    if "IP" in error_msg or "Connection" in error_msg or "timeout" in error_msg.lower():
                        if attempt < max_retries - 1:
                            # 动态调整重试等待时间，如果是因为IP超限，可能需要等久一点
                            wait_time = retry_delay
                            if "IP" in error_msg:
                                wait_time = max(5, retry_delay) # IP超限至少等5秒
                            
                            logger.warning(f"Tushare query failed (Attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            retry_delay *= 2  # Exponential backoff
                        else:
                            logger.error(f"Tushare query failed after {max_retries} attempts: {e}")
                            raise
                    else:
                        # 其他逻辑错误直接抛出
                        logger.error(f"Tushare query failed: {e}")
                        raise
        finally:
            self._semaphore.release()
