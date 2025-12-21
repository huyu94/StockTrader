from typing import Optional, List, Dict
import pandas as pd
from datetime import timedelta
from loguru import logger
from tqdm import tqdm
from src.loaders.adj_factor_loader import AdjFactorLoader
from src.fetchers.adj_factor_fetcher import AdjFactorFetcher

class AdjFactorManager:
    """复权因子管理器
    协调Loader和Fetcher，实现自动更新与缓存读取
    """
    def __init__(self, provider_name: str = "tushare"):
        self.loader = AdjFactorLoader()
        self.fetcher = AdjFactorFetcher(provider_name=provider_name)
        
    def get_adj_factor(self, ts_code: str) -> Optional[pd.DataFrame]:
        """
        获取指定股票的复权因子
        如果本地数据过期或缺失，会自动从远程拉取并更新缓存（默认拉取近一年）
        
        :param ts_code: 股票代码
        """
        if self.loader.check_update_needed(ts_code):
            # logger.info(f"Updating adj factor for {ts_code}...")
            
            # 计算默认日期范围：近一年
            now = pd.Timestamp.now()
            end_date = now.strftime("%Y%m%d")
            start_date = (now - timedelta(days=365)).strftime("%Y%m%d")
            
            df = self.fetcher.fetch_one(ts_code=ts_code, start_date=start_date, end_date=end_date, save_local=True)
            return df
        else:
            logger.debug(f"Loading adj factor from local for {ts_code}")
            return self.loader.load(ts_code)

    def batch_get_adj_factors(self, ts_codes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        批量获取复权因子
        
        :param ts_codes: 股票代码列表
        :return: 字典 {ts_code: DataFrame}
        """
        results = {}
        # 改回串行实现，避免Tushare IP限制问题
        for ts_code in tqdm(ts_codes, desc="Fetching adj factors"):
            try:
                df = self.get_adj_factor(ts_code)
                if df is not None and not df.empty:
                    results[ts_code] = df
            except Exception as e:
                logger.error(f"Error getting adj factor for {ts_code}: {e}")
        return results
