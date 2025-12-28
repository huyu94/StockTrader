import os
from typing import Optional
from datetime import timedelta
import pandas as pd
from loguru import logger
from tqdm import tqdm
from src.fetch.providers import BaseProvider, TushareProvider
from utils.date_helper import DateHelper
from src.storage.adj_factor_storage_mysql import AdjFactorStorageMySQL


class AdjFactorFetcher:
    """
    这里应该要分两段，一段是爬取除权除息日，然后爬取对应除权除息日的股票的复权因子
    """
    def __init__(self, 
        provider: BaseProvider,
        storage: AdjFactorStorageMySQL):
        self.provider = provider
        self.storage = storage




    def _fetch_ex_date(self, ex_date: str) -> pd.DataFrame:
        """
        爬取除权除息日
        """
        params = {}
        params['ex_date'] = ex_date
        params['fields'] = 'ts_code, ex_date'
        df = self.provider.query("dividend", **params)
        return df

    def _fetch_adj_factor(self, ts_codes: list[str], trade_date: str) -> pd.DataFrame:
        """
        """
        params = {}
        # params['ts_code'] = ts_code
        params['trade_date'] = trade_date
        params['fields'] = 'ts_code, trade_date, adj_factor'
        df = self.provider.query("adj_factor", **params)
        return df[df.ts_code.isin(ts_codes)]
        


    def fetch(self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
        ) -> pd.DataFrame:
        """
        查询除权除息日的复权因子数据
        
        :param start_date: 开始日期（YYYYMMDD）
        :param end_date: 结束日期（YYYYMMDD）
        :return: DataFrame，包含字段 trade_date, ts_code, ex_date, adj_factor
        """
        if not start_date or not end_date:
            raise ValueError("start_date 和 end_date 必须同时提供")
        
        # 标准化日期格式
        start_date_normalized = DateHelper.normalize_to_yyyymmdd(start_date)
        end_date_normalized = DateHelper.normalize_to_yyyymmdd(end_date)
        
        # 转换为 date 对象用于遍历
        start = DateHelper.parse_to_date(start_date_normalized)
        end = DateHelper.parse_to_date(end_date_normalized)
        
        if start > end:
            raise ValueError("start_date 不能大于 end_date")
        
        # 计算总天数
        total_days = (end - start).days + 1
        
        # 存储所有结果
        all_results = []
        
        # 遍历日期范围，使用 tqdm 显示进度
        current_date = start
        with tqdm(total=total_days, desc="查询除权除息数据", unit="天") as pbar:
            while current_date <= end:
                # 转换为 YYYYMMDD 格式用于 API 调用
                trade_date_str = current_date.strftime('%Y%m%d')
                
                # 更新进度条描述
                pbar.set_description(f"查询日期 {current_date.strftime('%Y-%m-%d')}")
                
                # 获取该日期的除权除息股票数据
                ex_date_df = self._fetch_ex_date(trade_date_str)
                ts_codes = ex_date_df['ts_code'].tolist()
                adj_factor_df = self._fetch_adj_factor(ts_codes, trade_date_str)
                result_df = adj_factor_df[adj_factor_df['ts_code'].isin(ts_codes)]
                if not result_df.empty:
                    all_results.append(result_df)
                # 移动到下一天
                current_date += timedelta(days=1)
                pbar.update(1)
        
        # 转换为 DataFrame
        if all_results:
            result_df = pd.concat(all_results).reset_index(drop=True)
            logger.info(f"共获取 {len(result_df)} 条复权因子数据")
            return result_df
        else:
            logger.info("未找到任何复权因子数据")
            return pd.DataFrame(columns=['trade_date', 'ts_code', 'ex_date', 'adj_factor']) 

    def to_storage_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        """
        return df

    def update(self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None):
        """
        """
        df = self.fetch(start_date, end_date)
        df_storage = self.to_storage_format(df)
        self.storage.write(df_storage)