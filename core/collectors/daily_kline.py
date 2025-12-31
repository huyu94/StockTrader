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
                - ts_codes: List[str] 或 str, 股票代码列表或单个代码（可选）
                - start_date: str, 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
                - end_date: str, 结束日期 (YYYY-MM-DD 或 YYYYMMDD)
                
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
        self._validate_params(params, required_keys=["start_date", "end_date"])
        
        ts_codes = params.get("ts_codes")
        start_date = params.get("start_date")  # 已经是 YYYYMMDD 格式
        end_date = params.get("end_date")  # 已经是 YYYYMMDD 格式
        
        # 统一处理 ts_codes
        if ts_codes is None:
            ts_codes_list = None
        elif isinstance(ts_codes, str):
            ts_codes_list = [ts_codes]
        elif isinstance(ts_codes, list):
            ts_codes_list = ts_codes if ts_codes else None
        else:
            raise CollectorException(f"ts_codes 必须是 str、List[str] 或 None，当前类型: {type(ts_codes)}")
        
        logger.info(f"开始采集日K线数据: ts_codes={ts_codes_list}, 日期范围={start_date}~{end_date}")
        
        provider = self._get_provider()
        
        # 判断采集模式
        if ts_codes_list is None or len(ts_codes_list) == 0:
            # 按日期模式：获取全市场数据
            return self._collect_by_date(provider, start_date, end_date)
        else:
            # 按代码模式：获取指定股票数据
            return self._collect_by_code(provider, ts_codes_list, start_date, end_date)
    
    def _collect_by_code(self, provider, ts_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        按股票代码采集数据（使用 pro_bar API，一次获取全部历史数据）
        
        Args:
            provider: 数据源提供者
            ts_codes: 股票代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            pd.DataFrame: 日K线数据
        """
        all_results = []
        
        for ts_code in tqdm(ts_codes, desc="采集日K线数据", unit="股票"):
            try:
                df = self._retry_collect(
                    provider.pro_bar,
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj="None",  # 爬取不复权数据
                    freq="D"
                )
                
                if df is not None and not df.empty:
                    all_results.append(df)
            except Exception as e:
                logger.error(f"采集股票 {ts_code} 失败: {e}")
                continue
        
        if all_results:
            result = pd.concat(all_results).reset_index(drop=True)
            logger.info(f"采集完成，共 {len(result)} 条记录")
            return result
        else:
            logger.warning("未采集到任何数据")
            return pd.DataFrame()
    
    def _collect_by_date(self, provider, start_date: str, end_date: str) -> pd.DataFrame:
        """
        按日期采集数据（使用 daily API，获取全市场数据）
        
        Args:
            provider: 数据源提供者
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            pd.DataFrame: 日K线数据
        """
        from datetime import timedelta
        
        start = DateHelper.parse_to_date(start_date)
        end = DateHelper.parse_to_date(end_date)
        
        total_days = (end - start).days + 1
        all_results = []
        current_date = start
        
        with tqdm(total=total_days, desc="按日期采集日K线", unit="天") as pbar:
            while current_date <= end:
                trade_date_str = current_date.strftime('%Y%m%d')
                pbar.set_description(f"查询日期 {current_date.strftime('%Y-%m-%d')}")
                
                try:
                    df = self._retry_collect(
                        provider.query,
                        "daily",
                        trade_date=trade_date_str
                    )
                    
                    if df is not None and not df.empty:
                        all_results.append(df)
                        logger.debug(f"日期 {current_date.strftime('%Y-%m-%d')}: 获取到 {len(df)} 条数据")
                except Exception as e:
                    logger.error(f"采集日期 {current_date.strftime('%Y-%m-%d')} 失败: {e}")
                
                current_date += timedelta(days=1)
                pbar.update(1)
        
        if all_results:
            result = pd.concat(all_results).reset_index(drop=True)
            logger.info(f"采集完成，共 {len(result)} 条记录")
            return result
        else:
            logger.warning("未采集到任何数据")
            return pd.DataFrame()

