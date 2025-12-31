"""
除权除息日采集器

负责从数据源采集除权除息日数据。
此采集器作为内部组件，主要用于支持 AdjFactorCollector。
"""

from typing import Any, Dict, List
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.collectors.basic_info import BasicInfoCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class ExDateCollector(BaseCollector):
    """
    除权除息日采集器
    
    从数据源采集股票的除权除息日信息。
    此采集器通常不作为独立 Pipeline 使用，而是作为其他采集器的内部组件。
    """
    def __init__(self, config: Dict[str, Any] = None, provider: Any = None):
        super().__init__(config, provider)
        self.basic_info_collector = BasicInfoCollector(config, provider)



    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集除权除息日数据
        
        Args:
            params: 采集参数，至少需要提供以下参数之一：
                - ts_code: str, 股票代码
                - ex_date: str, 除权除息日 (YYYYMMDD 或 YYYY-MM-DD)
                - ann_date: str, 公告日 (YYYYMMDD 或 YYYY-MM-DD)
                - record_date: str, 股权登记日期 (YYYYMMDD 或 YYYY-MM-DD)
                - imp_ann_date: str, 实施公告日 (YYYYMMDD 或 YYYY-MM-DD)
                - fields: str, 需要返回的字段，默认为 "ts_code,ex_date"
                
        Returns:
            pd.DataFrame: 除权除息日数据
            
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 参数验证和日期格式化（由 BaseCollector._validate_params 处理）
        self._validate_params(params)
        
        provider = self._get_provider()
        
        # 提取 fields 参数，如果没有则使用默认值
        fields = params.pop("fields", "ts_code,ex_date")
        
        # 处理其他日期参数的格式化（ann_date, record_date, imp_ann_date）
        date_params = ["ann_date", "record_date", "imp_ann_date"]
        for date_param in date_params:
            if date_param in params and params[date_param]:
                try:
                    params[date_param] = DateHelper.normalize_to_yyyymmdd(params[date_param])
                except ValueError as e:
                    raise CollectorException(f"{date_param} 格式错误: {e}")
        
        logger.info(f"开始采集除权除息日数据: params={params}")
        
        # 检查是否至少有一个查询参数
        query_params = ["ts_code", "ex_date", "ann_date", "record_date", "imp_ann_date"]
        if not any(params.get(key) for key in query_params):
            raise CollectorException("至少需要提供以下参数之一: ts_code, ex_date, ann_date, record_date, imp_ann_date")
        
        # 直接调用 API
        try:
            df = self._retry_collect(
                provider.query,
                "dividend",
                fields=fields,
                **params
            )
            
            if df is not None and not df.empty:
                logger.info(f"成功采集到 {len(df)} 条除权除息日数据")
                return df
            else:
                logger.info("未采集到任何除权除息日数据")
                return pd.DataFrame(columns=fields.split(","))
        except Exception as e:
            logger.error(f"采集除权除息日数据失败: {e}")
            raise CollectorException(f"采集除权除息日数据失败: {e}") from e

    def get_ex_dates_list(self, ts_code: str) -> List[str]:
        """
        获取指定股票的除权除息日列表（便捷方法，返回字符串列表）
        
        Args:
            ts_code: 股票代码
            
        Returns:
            List[str]: 除权除息日列表 (YYYYMMDD 格式)
        """
        df = self.collect({"ts_code": ts_code, "fields": "ts_code,ex_date"})
        
        if df.empty:
            return []
        
        # 过滤掉空值并转换为列表
        ex_dates = df[df['ex_date'].notna()]['ex_date'].tolist()
        return ex_dates

    def get_single_stock_ex_dates(self, ts_code: str) -> pd.DataFrame:
        """
        获取指定股票的除权除息日数据（便捷方法，返回 DataFrame）
        
        Args:
            ts_code: 股票代码
            
        Returns:
            pd.DataFrame: 除权除息日数据，包含 ts_code 和 ex_date 列
        """
        return self.collect({"ts_code": ts_code, "fields": "ts_code,ex_date"})

    def get_batch_stocks_ex_dates(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        获取指定股票列表的除权除息日数据
        
        Args:
            ts_codes: 股票代码列表
            
        Returns:
            pd.DataFrame: 合并后的除权除息日数据
        """
        all_results = []
        for ts_code in ts_codes:
            df = self.get_single_stock_ex_dates(ts_code)
            if not df.empty:
                all_results.append(df)
        
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame(columns=['ts_code', 'ex_date'])