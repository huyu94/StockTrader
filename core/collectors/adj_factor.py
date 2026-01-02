"""
复权因子采集器

负责从数据源采集复权因子数据
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.collectors.ex_date import ExDateCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class AdjFactorCollector(BaseCollector):
    """
    复权因子采集器
    
    从数据源采集股票的复权因子数据。
    只爬取除权除息日的复权因子
    """
    
    def __init__(self, config: Dict[str, Any] = None, provider: Any = None):
        """
        初始化复权因子采集器
        
        Args:
            config: 采集器配置
            provider: 数据源提供者实例
        """
        super().__init__(config, provider)
        self._ex_date_collector = ExDateCollector(config, provider)
    
    def collect(
        self, 
        ts_code: Optional[str] = None, 
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None, 
        fields: Optional[str] = "ts_code,trade_date,adj_factor",
        **kwargs
        ) -> pd.DataFrame:


        """
        采集复权因子数据
        
        Args:
            - ts_code: str, 股票代码
            - start_date: Optional[str], 开始日期 (YYYYMMDD 或 YYYY-MM-DD)
            - end_date: Optional[str], 结束日期 (YYYYMMDD 或 YYYY-MM-DD)
            - fields: Optional[str], 需要返回的字段，默认为 "ts_code,trade_date,adj_factor"

                
        Returns:
            pd.DataFrame: 复权因子数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - adj_factor: 复权因子
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 参数验证和日期格式化（由 BaseCollector._validate_params 处理）

        if start_date is not None:
            start_date = DateHelper.normalize_to_yyyymmdd(start_date)
        if end_date is not None:
            end_date = DateHelper.normalize_to_yyyymmdd(end_date)
        if trade_date is not None:
            trade_date = DateHelper.normalize_to_yyyymmdd(trade_date)
            
        provider = self._get_provider()

        # 构建查询参数
        query_params = {"ts_code": ts_code}
        if start_date is not None:
            query_params["start_date"] = start_date
        if end_date is not None:
            query_params["end_date"] = end_date
        if trade_date is not None:
            query_params["trade_date"] = trade_date
        
        # 直接调用 API，provider.query 内部已经有重试机制
        try:
            df = provider.query("adj_factor", fields=fields, **query_params)
            
            if df is not None and not df.empty:
                return df
            else:
                logger.debug(f"未采集到{ts_code}的任何复权因子数据")
                return pd.DataFrame(columns=fields.split(","))
        except Exception as e:
            logger.error(f"采集复权因子数据失败: {e}")
            raise CollectorException(f"采集复权因子数据失败: {e}") from e

    def get_single_stock_adj_factor(self, ts_code: str) -> pd.DataFrame:
        """
        获取指定股票的除权除息日对应的复权因子数据
        """

        ex_date_df = self._ex_date_collector.get_single_stock_ex_dates(ts_code)
        if ex_date_df.empty:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])
        adj_factor_df = self.collect(ts_code=ts_code, fields="ts_code,trade_date,adj_factor")
        if adj_factor_df.empty:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])
        # 拼接
        result_df = pd.merge(ex_date_df, adj_factor_df, left_on='ex_date', right_on='trade_date', how='left')
        result_df = result_df[result_df.adj_factor.notna()].drop(columns=['ts_code_y', 'ex_date'])
        result_df = result_df.rename(columns={'ts_code_x': 'ts_code'})
        return result_df
    
    def get_batch_stocks_adj_factor(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        获取指定股票列表的复权因子数据
        
        Args:
            ts_codes: 股票代码列表
            
        Returns:
            pd.DataFrame: 合并后的复权因子数据
        """
        all_results = []
        for ts_code in ts_codes:
            df = self.get_single_stock_adj_factor(ts_code)
            if not df.empty:
                all_results.append(df)
        
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame(columns=['ts_code', 'trade_date', 'adj_factor'])