"""
交易日历采集器

负责从数据源采集交易日历数据
"""

from typing import Any, Dict, Optional
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class TradeCalendarCollector(BaseCollector):
    """
    交易日历采集器
    
    从数据源采集交易日历信息，用于判断某日期是否为交易日
    """
    
    def collect(
        self,
        start_date: str,
        end_date: str,
        exchange: Optional[str] = "SSE",
        **kwargs
    ) -> pd.DataFrame:
        """
        采集交易日历数据
        
        Args:
            start_date: str, 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: str, 结束日期 (YYYY-MM-DD 或 YYYYMMDD)
            exchange: str, 交易所代码 (可选，默认 'SSE')
            **kwargs: 其他参数
        """
        query_params = {
            "exchange": exchange,
            "start_date": start_date,
            "end_date": end_date
        }
                
        # 验证参数（会自动标准化日期格式为 YYYYMMDD）
        self._validate_params(query_params, required_keys=["start_date", "end_date"])
    
        provider = self._get_provider()
        
        try:
            # 直接调用 provider.query，它内部已经有重试机制
            df = provider.query("trade_cal", **query_params)
            
            if df is not None and not df.empty:
                # 按日期排序
                df = df.sort_values("cal_date").reset_index(drop=True)
                logger.debug(f"采集完成，共 {len(df)} 条交易日历数据")
                return df
            else:
                logger.warning("未采集到交易日历数据")
                return pd.DataFrame(columns=["exchange", "cal_date", "is_open"])
        except Exception as e:
            raise CollectorException(f"采集交易日历失败: {e}") from e

