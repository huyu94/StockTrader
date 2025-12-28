import os
from typing import Optional
import pandas as pd
from loguru import logger
from src.fetch.providers.tushare_provider import BaseProvider, TushareProvider
from src.storage.calendar_storage_mysql import CalendarStorageMySQL
from utils.date_helper import DateHelper

import dotenv
dotenv.load_dotenv()

class CalendarFetcher:
    """交易日历获取器"""
    def __init__(self,
        provider: Optional[BaseProvider] = None,
        storage: Optional[CalendarStorageMySQL] = None,
    ):
        self.provider = provider
        self.storage = storage
        
        
    def fetch(self, start_date: Optional[str], end_date: Optional[str], exchange: str='SSE') -> pd.DataFrame:
        """
        获取交易日历
        
        :param start_date: 开始日期（YYYY-MM-DD 格式，内部统一格式）
        :param end_date: 结束日期（YYYY-MM-DD 格式，内部统一格式）
        :param exchange: 交易所代码
        """
        
        # Tushare API 需要 YYYYMMDD 格式，转换日期
        params = {'exchange': exchange}
        if start_date:
            params["start_date"] = DateHelper.normalize_to_yyyymmdd(start_date)
        if end_date:
            params["end_date"] = DateHelper.normalize_to_yyyymmdd(end_date)
        df = self.provider.query("trade_cal", **params)
        if df is not None and not df.empty:
            df = df.sort_values("cal_date").reset_index(drop=True)
            
        return df

    @staticmethod
    def to_storage_format(df: pd.DataFrame):
        """
        将网上爬取的交易日历数据转换为存储格式（宽表格式）
        将多个交易所的数据按日期分组，每个日期一行，多个交易所作为列
        
        :param df: 网上爬取的交易日历数据，包含 cal_date, is_open, exchange 列
        :return: 存储格式，包含 cal_date 和多个交易所的 _open 列
        """
        if df is None or df.empty:
            logger.warning(f"df 为空")
            return pd.DataFrame()
        
        # 确保 cal_date 是字符串格式（YYYYMMDD）
        df_copy = df.copy()
        if 'cal_date' in df_copy.columns:
            df_copy['cal_date'] = df_copy['cal_date'].apply(
                lambda x: DateHelper.normalize_to_yyyymmdd(str(x)) if pd.notna(x) else None
            )
        
        # 创建 exchange_open 列
        df_copy['exchange_open'] = df_copy.apply(
            lambda row: f"{row['exchange'].lower()}_open" if pd.notna(row.get('exchange')) else None,
            axis=1
        )
        # 使用 pivot 将数据转换为宽表格式
        # 将 exchange_open 作为列名，is_open 作为值
        pivot_df = df_copy.pivot_table(
            index='cal_date',
            columns='exchange',
            values='is_open',
            aggfunc='first'  # 如果同一日期同一交易所有多条记录，取第一条
        )
        
        # 重命名列为 {exchange}_open 格式
        pivot_df.columns = [f"{col.lower()}_open" for col in pivot_df.columns]
        
        # 重置索引，使 cal_date 成为列
        pivot_df = pivot_df.reset_index()
        
        # 确保 cal_date 是字符串格式
        pivot_df['cal_date'] = pivot_df['cal_date'].astype(str)
        
        # 排序并重置索引
        pivot_df = pivot_df.sort_values("cal_date").reset_index(drop=True)
        
        return pivot_df


    def upadte(self, start_date: Optional[str], end_date: Optional[str], exchange: str='SSE') -> pd.DataFrame:
        """
        更新交易日历
        :param start_date: 开始日期（YYYY-MM-DD 格式，内部统一格式）
        :param end_date: 结束日期（YYYY-MM-DD 格式，内部统一格式）
        :param exchange: 交易所代码
        """
        logger.debug(f"Updating calendar for {exchange} from {start_date} to {end_date}")
        df = self.fetch(start_date, end_date, exchange)
        df_storage = self.to_storage_format(df)
        self.storage.write_df(df_storage)


