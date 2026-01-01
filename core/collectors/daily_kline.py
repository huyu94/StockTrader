"""
日K线数据采集器

负责从数据源采集日K线数据
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from loguru import logger
from tqdm import tqdm

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class DailyKlineCollector(BaseCollector):
    """
    日K线数据采集器
    
    从数据源（Tushare、Akshare等）采集股票的日K线数据
    """
    
    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集日K线数据
        
        Args:
            params: 采集参数
                - trade_date: str, 交易日期 (YYYY-MM-DD 或 YYYYMMDD)

                
        Returns:
            pd.DataFrame: 日K线数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - open: 开盘价
                - high: 最高价
                - low: 最低价
                - close: 收盘价
                - vol: 成交量
                - amount: 成交额
                - 其他字段
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 验证参数（会自动标准化日期格式为 YYYYMMDD）
        self._validate_params(params, required_keys=["trade_date"])
        
        trade_date = params.get("trade_date")
        trade_date_str = DateHelper.normalize_to_yyyymmdd(trade_date)
        logger.info(f"开始采集日K线数据: 交易日期={trade_date}")
        
        provider = self._get_provider()
        
        try:
            df = self._retry_collect(
                provider.query,
                "daily",
                trade_date=trade_date_str
            )
            if df is not None and not df.empty:
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"采集日K线数据失败: {e}")
            return pd.DataFrame()