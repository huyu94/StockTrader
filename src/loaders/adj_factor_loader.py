import os
from datetime import datetime
from typing import Optional, Dict
import pandas as pd
from loguru import logger
from project_var import DATA_DIR, CACHE_DIR
import dotenv

dotenv.load_dotenv()

class AdjFactorLoader:
    """复权因子加载器
    负责读取本地数据，并基于CSV缓存文件判断是否需要更新
    """
    def __init__(self):
        # 数据目录
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        self.factor_dir = os.path.join(self.data_dir, "adj_factor")
        os.makedirs(self.factor_dir, exist_ok=True)
        
        # 缓存目录
        self.cache_dir = CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, "adj_factor_cache.csv")
        
        # 内存缓存：ts_code -> last_updated_at
        self._cache_map = {}
        self._load_cache_file()

    def _load_cache_file(self):
        """加载缓存CSV到内存"""
        if os.path.exists(self.cache_file):
            try:
                df = pd.read_csv(self.cache_file, dtype=str)
                if "ts_code" in df.columns and "last_updated_at" in df.columns:
                    self._cache_map = dict(zip(df["ts_code"], df["last_updated_at"]))
            except Exception as e:
                logger.warning(f"Failed to load adj_factor cache file: {e}")

    def _get_data_path(self, ts_code: str) -> str:
        return os.path.join(self.factor_dir, f"{ts_code}.csv")

    def load(self, ts_code: str) -> Optional[pd.DataFrame]:
        """
        读取本地复权因子数据
        """
        path = self._get_data_path(ts_code)
        if not os.path.exists(path):
            logger.warning(f"Adj factor file not found for {ts_code}: {path}")
            return None
            
        try:
            df = pd.read_csv(path, dtype={"trade_date": str})
            return df
        except Exception as e:
            logger.error(f"Failed to load adj factor for {ts_code}: {e}")
            return None

    def check_update_needed(self, ts_code: str) -> bool:
        """
        判断是否需要更新数据
        策略：
        1. 本地数据文件不存在 -> True
        2. 缓存记录不存在 -> True
        3. 缓存的最后更新日期不是今天 -> True
        """
        data_path = self._get_data_path(ts_code)
        if not os.path.exists(data_path):
            logger.debug(f"Update needed for {ts_code}: data file missing")
            return True
        
        # 刷新缓存映射（防止多进程或外部修改）
        # 优化：每次检查都读文件可能太慢，可以考虑仅在内存为空时读，或定时刷新
        # 这里为了准确性，每次重新加载缓存文件（假设文件不大）
        self._load_cache_file()
        
        last_updated = self._cache_map.get(ts_code)
        if not last_updated:
            logger.debug(f"Update needed for {ts_code}: cache record missing")
            return True
            
        try:
            # 统一解析格式为 %Y-%m-%d
            try:
                last_date = datetime.strptime(last_updated, "%Y-%m-%d").date()
            except ValueError:
                # 兼容旧格式 %Y-%m-%d %H:%M:%S
                last_date = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").date()

            today = datetime.now().date()
            
            if last_date < today:
                logger.debug(f"Update needed for {ts_code}: cache expired ({last_date} < {today})")
                return True
        except Exception as e:
            logger.warning(f"Error parsing date for {ts_code}: {e}")
            return True
            
        return False
