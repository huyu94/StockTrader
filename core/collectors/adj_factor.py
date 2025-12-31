"""
复权因子采集器

负责从数据源采集复权因子数据
"""

from typing import Any, Dict, List, Optional
from datetime import timedelta
import pandas as pd
from loguru import logger
from pandas._libs.join import left_outer_join
from tqdm import tqdm

from core.collectors.base import BaseCollector
from core.collectors.ex_date import ExDateCollector
from core.collectors.basic_info import BasicInfoCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class AdjFactorCollector(BaseCollector):
    """
    复权因子采集器
    
    从数据源采集股票的复权因子数据。
    依赖 ExDateCollector 获取除权除息日信息。
    """
    
    def __init__(self, config: Dict[str, Any] = None, provider: Any = None):
        """
        初始化复权因子采集器
        
        Args:
            config: 采集器配置
            provider: 数据源提供者实例
        """
        super().__init__(config, provider)
        # 组合模式：内部使用 ExDateCollector
        self.ex_date_collector = ExDateCollector(config, provider)
        self.basic_info_collector = BasicInfoCollector(config, provider)
        logger.debug("初始化复权因子采集器，已组合 ExDateCollector")
    
    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集复权因子数据
        
        Args:
            params: 采集参数
                - ts_codes: List[str], 股票代码列表 (可选，不提供则需要先获取股票列表)
                
        Returns:
            pd.DataFrame: 复权因子数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - adj_factor: 复权因子
                - 其他字段
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 获取参数
        ts_codes = params.get('ts_codes', None)  # 股票代码列表参数（可选）
        
        logger.info(f"开始采集复权因子数据: 股票代码列表={ts_codes if ts_codes else '未提供'}")
        
        provider = self._get_provider()
        
        # 如果没有提供股票代码列表，需要先获取所有股票代码
        if ts_codes is None:
            logger.info("没有提供股票代码列表，需要先获取所有股票代码..")
            basic_info_df = self.basic_info_collector.collect({})
            ts_codes = basic_info_df['ts_code'].tolist() if not basic_info_df.empty else []
            logger.info(f"获取到 {len(ts_codes)} 条股票代码")

        if not ts_codes:
            logger.error("没有获取到股票代码，无法采集复权因子数据")
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])

        all_result = []

        with tqdm(total=len(ts_codes), desc="采集复权因子", unit="股票") as pbar:
            for ts_code in ts_codes:
                pbar.set_description(f"采集股票 {ts_code} 的复权因子")
                ex_dates = self._get_ex_dates_for_stock(provider, ts_code)
                if not ex_dates.empty:
                    adj_factors = self._collect_adj_factors_for_stock(provider, ts_code)
                    if not adj_factors.empty:
                        result_df = pd.merge(ex_dates, adj_factors, left_on='ex_date', right_on='trade_date', how='inner')
                        
                        result_df = result_df[['ts_code_x', 'trade_date', 'adj_factor']].copy()
                        result_df.columns = ['ts_code', 'trade_date', 'adj_factor']

                        result_df = result_df[result_df['adj_factor'].notna()]
                        



                        all_result.append(result_df)
                pbar.update(1)
        


        if all_result:
            result = pd.concat(all_result).reset_index(drop=True)
            logger.info(f"共采集到 {len(result)} 条复权因子数据")
            return result
        else:
            logger.info("未采集到任何复权因子数据")
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])


    
    
    def _get_ex_dates_for_stock(self, provider, ts_code: str):
        """
        获取指定股票的除权除息日
        
        Args:
            provider: 数据源提供者
            ts_code: 股票代码
            
        Returns:
            pd.DataFrame: 除权除息日数据
        """
        try:
            df = self._retry_collect(
                provider.query,
                "dividend",
                ts_code=ts_code,
                fields="ts_code,ex_date"
            )

            if df is not None and not df.empty:
                # 过滤日期范围，只保留在指定范围内的除权除息日
                return df[df['ex_date'].notna()]
            else:
                return pd.DataFrame(columns=['ts_code', 'ex_date'])
        except Exception as e:
            logger.error(f"获取 {ts_code} 的除权除息日失败: {e}")
            return pd.DataFrame(columns=['ts_code', 'ex_date'])


    def _collect_adj_factors_for_stock(self, provider, ts_code: str):
        """
        获取指定股票的全部历史复权因子

        Args:
            provider: 数据源提供者
            ts_code: 股票代码


        Returns:
        pd.DataFrame: 复权因子数据， 包含ts_code, trade_date, adj_factor列

        """

        try:
            df = self._retry_collect(
                provider.query,
                "adj_factor",
                ts_code=ts_code,
                fields="ts_code,trade_date,adj_factor"
            )
            
            if df is not None and not df.empty:
                return df
            else:
                return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])
        except Exception as e:
            logger.error(f"获取 {ts_code} 的全部历史复权因子失败: {e}")
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])