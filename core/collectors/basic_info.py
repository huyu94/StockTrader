"""
股票基本信息采集器

负责从数据源采集股票基本信息数据
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException


class BasicInfoCollector(BaseCollector):
    """
    股票基本信息采集器
    
    从数据源采集股票的基本信息，如股票代码、名称、上市日期等
    """
    
    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集股票基本信息
        
        Args:
            params: 采集参数
                - stock_codes: List[str], 股票代码列表 (可选，不提供则采集全部)
                - exchange: str, 交易所代码 (可选，SSE/SZSE/BSE)
                - market: str, 市场类别 (可选，主板/创业板/科创板/CDR/北交所)
                - is_hs: str, 是否沪深港通标的 (可选，N否/H沪股通/S深股通)
                - list_status: str, 上市状态 (可选，默认 "L" 表示上市)
                - fields: str, 字段列表 (可选，默认包含所有常用字段)
                
        Returns:
            pd.DataFrame: 股票基本信息数据，包含以下列：
                - ts_code: 股票代码
                - symbol: 股票代码（简化）
                - name: 股票名称
                - area: 地域
                - industry: 所属行业
                - market: 市场类型
                - list_date: 上市日期
                - 其他字段
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 提取参数
        stock_codes = params.get("stock_codes")
        exchange = params.get("exchange")
        market = params.get("market")
        is_hs = params.get("is_hs")
        list_status = params.get("list_status", "L")
        fields = params.get("fields")
        
        # 默认字段
        if not fields:
            fields = "ts_code,symbol,name,area,industry,market,list_date,list_status,is_hs,exchange"
        
        logger.info(f"开始采集股票基本信息: list_status={list_status}, exchange={exchange}, market={market}, is_hs={is_hs}")
        
        provider = self._get_provider()
        
        # 构建查询参数
        query_params = {"list_status": list_status}
        if exchange:
            query_params["exchange"] = exchange
        if market:
            query_params["market"] = market
        if is_hs:
            query_params["is_hs"] = is_hs
        
        try:
            df = self._retry_collect(
                provider.query,
                "stock_basic",
                fields=fields,
                **query_params
            )
            
            if df is not None and not df.empty:
                # 如果指定了股票代码，进行过滤
                if stock_codes:
                    if isinstance(stock_codes, str):
                        stock_codes = [stock_codes]
                    df = df[df['ts_code'].isin(stock_codes)]
                
                logger.info(f"采集完成，共 {len(df)} 条股票基本信息")
                return df
            else:
                logger.warning("未采集到股票基本信息")
                return pd.DataFrame()
        except Exception as e:
            raise CollectorException(f"采集股票基本信息失败: {e}") from e

    def get_single_stock_basic_info(self, ts_code: str) -> pd.DataFrame:
        """
        获取指定股票的基本信息
        """
        return self.collect({"ts_code": ts_code, "fields": "ts_code,symbol,name,area,industry,market,list_date,list_status,is_hs,exchange"})
    
    def get_batch_stocks_basic_info(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        获取指定股票列表的基本信息
        """
        all_results = []
        for ts_code in ts_codes:
            df = self.get_single_stock_basic_info(ts_code)
            if not df.empty:
                all_results.append(df)
        
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame(columns=['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'list_date', 'list_status', 'is_hs', 'exchange'])

    def get_all_ts_codes(self) -> List[str]:
        """
        获取所有股票代码
        """
        df = self.collect({})
        return df['ts_code'].tolist()