import os
import json
import pandas as pd
from datetime import datetime
from typing import Optional, Dict
from loguru import logger
from project_var import DATA_DIR, CACHE_DIR
import dotenv

dotenv.load_dotenv()

class CalendarLoader:
    """交易日历加载器
    负责读取本地数据，并判断是否需要更新
    """
    def __init__(self):
        # 数据目录
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 缓存目录
        self.cache_dir = CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, exchange: str) -> str:
        return os.path.join(self.cache_dir, f"calendar_{exchange}_update.json")
        
    def _get_data_path(self, exchange: str) -> str:
        return os.path.join(self.data_dir, f"{exchange}_trade_calendar.csv")

    def load(self, exchange: str) -> Optional[pd.DataFrame]:
        """
        读取本地交易日历数据
        """
        path = self._get_data_path(exchange)
        if not os.path.exists(path):
            logger.warning(f"Calendar file not found for {exchange}: {path}")
            return None
            
        try:
            # 明确指定cal_date为字符串，防止被自动识别为数字
            df = pd.read_csv(path, dtype={"cal_date": str})
            return df
        except Exception as e:
            logger.error(f"Failed to load calendar for {exchange}: {e}")
            return None

    def check_update_needed(self, exchange: str) -> bool:
        """
        判断是否需要更新数据
        策略：
        1. 本地数据文件不存在 -> True
        2. 缓存文件不存在 -> True
        3. 缓存的最后更新日期不是今天 -> True
        """
        data_path = self._get_data_path(exchange)
        if not os.path.exists(data_path):
            logger.debug(f"Update needed for {exchange}: data file missing")
            return True
            
        cache_path = self._get_cache_path(exchange)
        if not os.path.exists(cache_path):
            logger.debug(f"Update needed for {exchange}: cache file missing")
            return True
            
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                info = json.load(f)
                last_updated = info.get("last_updated_at", "")
                if not last_updated:
                    return True
                
                # 统一解析格式为 %Y-%m-%d
                last_date = datetime.strptime(last_updated, "%Y-%m-%d").date()
                today = datetime.now().date()
                
                if last_date < today:
                    logger.debug(f"Update needed for {exchange}: cache expired ({last_date} < {today})")
                    return True
                    
        except Exception as e:
            logger.warning(f"Error checking cache for {exchange}: {e}")
            return True
            
        return False
