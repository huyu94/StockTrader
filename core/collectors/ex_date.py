"""
除权除息日采集器

负责从数据源采集除权除息日数据。
此采集器作为内部组件，主要用于支持 AdjFactorCollector。
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class ExDateCollector(BaseCollector):
    """
    除权除息日采集器
    
    从数据源采集股票的除权除息日信息。
    此采集器通常不作为独立 Pipeline 使用，而是作为其他采集器的内部组件。
    """
    
    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集除权除息日数据
        
        Args:
            params: 采集参数
                - ex_date: str, 除权除息日 (YYYY-MM-DD 或 YYYYMMDD)
                - stock_code: str, 股票代码 (可选)
                
        Returns:
            pd.DataFrame: 除权除息日数据，包含以下列：
                - ts_code: 股票代码
                - ex_date: 除权除息日
                - 其他字段
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 验证参数（会自动标准化日期格式为 YYYYMMDD）
        self._validate_params(params, required_keys=["ex_date"])
        
        ex_date = params.get("ex_date")  # 已经是 YYYYMMDD 格式
        stock_code = params.get("stock_code")
        
        logger.info(f"开始采集除权除息日数据: ex_date={ex_date}, stock_code={stock_code}")
        
        provider = self._get_provider()
        
        # 调用 Tushare dividend API
        query_params = {
            "ex_date": ex_date,
            "fields": "ts_code,ex_date"
        }
        
        if stock_code:
            query_params["ts_code"] = stock_code
        
        try:
            df = self._retry_collect(
                provider.query,
                "dividend",
                **query_params
            )
            
            if df is not None and not df.empty:
                logger.info(f"采集到 {len(df)} 条除权除息日数据")
                return df
            else:
                logger.warning("未采集到除权除息日数据")
                return pd.DataFrame(columns=["ts_code", "ex_date"])
        except Exception as e:
            raise CollectorException(f"采集除权除息日数据失败: {e}") from e
    
    def get_ex_dates_list(self, stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[str]:
        """
        获取指定股票的除权除息日列表（便捷方法）
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期 (可选)
            end_date: 结束日期 (可选)
            
        Returns:
            List[str]: 除权除息日列表 (YYYY-MM-DD 格式)
        """
        # 如果提供了日期范围，需要遍历日期范围
        if start_date and end_date:
            from datetime import timedelta
            
            start = DateHelper.parse_to_date(start_date)
            end = DateHelper.parse_to_date(end_date)
            ex_dates = []
            
            current_date = start
            while current_date <= end:
                trade_date_str = current_date.strftime('%Y%m%d')
                try:
                    df = self.collect({"ex_date": trade_date_str, "stock_code": stock_code})
                    if not df.empty:
                        # 提取该股票的除权除息日
                        stock_ex_dates = df[df["ts_code"] == stock_code]["ex_date"].tolist()
                        ex_dates.extend(stock_ex_dates)
                except Exception as e:
                    logger.debug(f"查询日期 {current_date.strftime('%Y-%m-%d')} 的除权除息日失败: {e}")
                
                current_date += timedelta(days=1)
            
            # 去重并转换为 YYYY-MM-DD 格式
            unique_ex_dates = list(set(ex_dates))
            normalized_dates = [DateHelper.normalize_to_yyyy_mm_dd(d) for d in unique_ex_dates]
            return sorted(normalized_dates)
        else:
            # 如果没有日期范围，返回空列表（需要提供日期范围才能查询）
            logger.warning("get_ex_dates_list 需要提供 start_date 和 end_date 参数")
            return []

