from typing import Optional, List
import pandas as pd
from functools import cached_property
from loguru import logger
from src.loaders.basic_info_loader import BasicInfoLoader
from src.fetchers.basic_info_fetcher import BasicInfoFetcher

class BasicInfoManager:
    """股票基本信息管理器
    协调Loader和Fetcher，实现自动更新与缓存读取
    """
    def __init__(self, provider_name: str = "tushare"):
        self.loader = BasicInfoLoader()
        self.fetcher = BasicInfoFetcher(provider_name=provider_name)
        
    def _get_all_stocks(self) -> pd.DataFrame:
        """
        获取所有股票基本信息（默认拉取全市场）
        如果本地数据过期或缺失，则自动从远程获取
        """
        if self.loader.check_update_needed():
            logger.info("更新股票基本信息列表...")
            # fetcher 会负责拉取、保存文件、更新 cache
            # 默认拉取所有交易所
            df = self.fetcher.fetch(save_local=True)
            return df
        else:
            logger.debug("从本地加载股票基本信息列表")
            return self.loader.load()

    @cached_property
    def all_basic_info(self) -> pd.DataFrame:
        """缓存的股票基本信息（全市场）"""
        return self._get_all_stocks()

    @cached_property
    def all_stock_codes(self) -> list[str]:
        """缓存的所有股票代码"""
        if self.all_basic_info is not None:
            return self.all_basic_info["ts_code"].tolist()
        return []

    def get_stocks_by_market(self, markets: List[str]) -> pd.DataFrame:
        """按市场类别筛选股票 (如 主板/创业板/科创板/CDR/北交所)"""
        df = self.all_basic_info
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Tushare market 字段值示例：主板, 创业板, 科创板, CDR, 北交所
        return df[df["market"].isin(markets)]

    def get_stocks_by_exchange(self, exchanges: List[str]) -> pd.DataFrame:
        """按交易所筛选股票 (如 SSE, SZSE, BSE)"""
        df = self.all_basic_info
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Tushare exchange 字段值示例：SSE, SZSE, BSE
        # 注意：fetcher fetch 时需要确保 exchange 字段被包含在内
        if "exchange" not in df.columns:
             logger.warning("Basic info dataframe missing 'exchange' column")
             return pd.DataFrame()

        return df[df["exchange"].isin(exchanges)]


    def get_stocks_code_by_market(self, markets: List[str]) -> List[str]:
        """按市场类别筛选股票代码 (如 主板/创业板/科创板/CDR/北交所)"""
        df = self.get_stocks_by_market(markets)
        if df is None or df.empty:
            return []
        return df["ts_code"].tolist()
    
    def get_stocks_code_by_exchange(self, exchanges: List[str]) -> List[str]:
        """按交易所筛选股票代码 (如 SSE, SZSE, BSE)"""
        df = self.get_stocks_by_exchange(exchanges)
        if df is None or df.empty:
            return []
        return df["ts_code"].tolist()
