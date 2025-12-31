"""
复权因子采集器

负责从数据源采集复权因子数据
"""

from typing import Any, Dict, List
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException


class AdjFactorCollector(BaseCollector):
    """
    复权因子采集器
    
    从数据源采集股票的复权因子数据。
    """
    
    def __init__(self, config: Dict[str, Any] = None, provider: Any = None):
        """
        初始化复权因子采集器
        
        Args:
            config: 采集器配置
            provider: 数据源提供者实例
        """
        super().__init__(config, provider)
    
    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集复权因子数据
        
        Args:
            params: 采集参数，至少需要提供以下参数之一：
                - ts_code: str, 股票代码
                - trade_date: str, 交易日期 (YYYYMMDD 或 YYYY-MM-DD)
                - start_date: str, 开始日期 (YYYYMMDD 或 YYYY-MM-DD)
                - end_date: str, 结束日期 (YYYYMMDD 或 YYYY-MM-DD)
                - fields: str, 需要返回的字段，默认为 "ts_code,trade_date,adj_factor"
                
        Returns:
            pd.DataFrame: 复权因子数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - adj_factor: 复权因子
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 参数验证和日期格式化（由 BaseCollector._validate_params 处理）
        self._validate_params(params)
        
        provider = self._get_provider()
        
        # 提取 fields 参数，如果没有则使用默认值
        fields = params.pop("fields", "ts_code,trade_date,adj_factor")
        
        logger.info(f"开始采集复权因子数据: params={params}")
        
        # 检查是否至少有一个查询参数
        query_params = ["ts_code", "trade_date", "start_date", "end_date"]
        if not any(params.get(key) for key in query_params):
            raise CollectorException("至少需要提供以下参数之一: ts_code, trade_date, start_date, end_date")
        
        # 直接调用 API
        try:
            df = self._retry_collect(
                provider.query,
                "adj_factor",
                fields=fields,
                **params
            )
            
            if df is not None and not df.empty:
                logger.info(f"成功采集到 {len(df)} 条复权因子数据")
                return df
            else:
                logger.info("未采集到任何复权因子数据")
                return pd.DataFrame(columns=fields.split(","))
        except Exception as e:
            logger.error(f"采集复权因子数据失败: {e}")
            raise CollectorException(f"采集复权因子数据失败: {e}") from e


    def get_single_stock_adj_factor(self, ts_code: str) -> pd.DataFrame:
        """
        获取指定股票的复权因子数据
        """
        return self.collect({"ts_code": ts_code, "fields": "ts_code,trade_date,adj_factor"})
    
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