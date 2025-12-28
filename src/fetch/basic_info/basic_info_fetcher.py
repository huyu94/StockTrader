import os
from typing import Optional
import pandas as pd
from loguru import logger
from src.fetch.providers.tushare_provider import BaseProvider, TushareProvider
from src.storage.basic_info_storage_mysql import BasicInfoStorageMySQL


class BasicInfoFetcher:
    """股票基本信息获取器"""
    def __init__(self, 
        provider: BaseProvider,
        storage: BasicInfoStorageMySQL 
        ):
        self.provider = provider
        self.storage = storage
        
    def fetch(self, list_status: str = "L", 
                    exchange: str = None,
                    market: str = None,
                    is_hs: str = None,
                    fields: Optional[str] = None, 
                    ) -> pd.DataFrame:

        """
        获取股票基本信息
        exchange: 交易所 SSE上交所 SZSE深交所 BSE北交所
        market: 市场类别 （主板/创业板/科创板/CDR/北交所）
        is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
        """
        
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
        
        return df

    def to_storage_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将基本信息数据转换为存储格式
        """
        return df

    def update(self):
        """
        更新基本信息
        """
        df = self.fetch()
        df_storage = self.to_storage_format(df)
        self.storage.write(df_storage)
