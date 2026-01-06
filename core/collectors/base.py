"""
采集器基类模块

定义所有数据采集器的抽象基类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import time
import pandas as pd
from loguru import logger

from core.common.exceptions import CollectorException, NetworkException
from core.providers.tushare_provider import TushareProvider

class BaseCollector(ABC):
    """
    采集器抽象基类
    
    所有数据采集器都应该继承此类并实现 collect 方法。
    基类提供配置管理、重试机制等通用功能框架。
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, provider: Any = None):
        """
        初始化采集器
        
        Args:
            config: 采集器配置字典，包含数据源、重试次数、超时时间等配置
            provider: 数据源提供者实例（如 TushareProvider），如果为 None 则从配置中创建
        """
        self.config = config or {}
        self.source = self.config.get("source", "tushare")
        self.retry_times = self.config.get("retry_times", 3)
        self.timeout = self.config.get("timeout", 30)
        self.provider = provider
        logger.debug(f"初始化采集器: {self.__class__.__name__}, 数据源: {self.source}")
    
    @abstractmethod
    def collect(self, **kwargs) -> pd.DataFrame:
        """
        采集数据的核心方法（单次 API 调用）
        
        每个子类应该定义自己的参数签名，例如：
        - BasicInfoCollector: collect(exchange: Optional[str] = None, ...)
        - DailyKlineCollector: collect(trade_date: str, ...)
        - TradeCalendarCollector: collect(start_date: str, end_date: str, ...)
        
        Returns:
            pd.DataFrame: 采集到的原始数据
            
        Raises:
            CollectorException: 当采集失败时抛出异常
        """
        pass
    
    def _retry_collect(self, collect_func, *args, **kwargs) -> pd.DataFrame:
        """
        带重试机制的数据采集（已弃用）
        
        注意：此方法已不再使用，因为 TushareProvider.query() 内部已经有重试机制。
        如果将来需要为其他 provider 或直接调用 API 添加重试，可以重新启用此方法。
        
        Args:
            collect_func: 采集函数
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            pd.DataFrame: 采集到的数据
            
        Raises:
            CollectorException: 重试次数用尽后抛出异常
        """
        last_exception = None
        retry_delay = 2  # 重试延迟（秒）
        
        for attempt in range(self.retry_times):
            try:
                start_time = time.time()
                result = collect_func(*args, **kwargs)
                elapsed = time.time() - start_time
                logger.debug(f"采集成功 (尝试 {attempt + 1}/{self.retry_times}), 耗时: {elapsed:.3f}s")
                return result
            except Exception as e:
                last_exception = e
                elapsed = time.time() - start_time
                
                if attempt < self.retry_times - 1:
                    logger.warning(
                        f"采集失败 (尝试 {attempt + 1}/{self.retry_times}), "
                        f"耗时: {elapsed:.3f}s, 错误: {e}. {retry_delay}秒后重试..."
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"采集失败，已重试 {self.retry_times} 次, "
                        f"耗时: {elapsed:.3f}s, 错误: {e}"
                    )
                    raise CollectorException(f"采集失败，已重试 {self.retry_times} 次: {e}") from e
        
        # 如果所有重试都失败
        raise CollectorException(f"采集失败，已重试 {self.retry_times} 次") from last_exception
    
    def _validate_params(self, params: Dict[str, Any], required_keys: list = None, normalize_dates: bool = True) -> bool:
        """
        验证采集参数，并自动标准化日期格式
        
        Args:
            params: 待验证的参数（会被原地修改，日期会被标准化为 YYYYMMDD 格式）
            required_keys: 必需的参数键列表
            normalize_dates: 是否标准化日期格式（默认 True）
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            CollectorException: 参数验证失败时抛出异常
        """
        if required_keys is None:
            required_keys = []
        
        for key in required_keys:
            if key not in params or params[key] is None:
                raise CollectorException(f"缺少必需的参数: {key}")
        
        from utils.date_helper import DateHelper
        
        # 验证并标准化日期格式（如果存在）
        if normalize_dates:
            if "start_date" in params and params["start_date"]:
                try:
                    # 验证格式
                    DateHelper.normalize_to_yyyy_mm_dd(params["start_date"])
                    # 标准化为 YYYYMMDD 格式（API 需要）
                    params["start_date"] = DateHelper.normalize_to_yyyymmdd(params["start_date"])
                except ValueError as e:
                    raise CollectorException(f"start_date 格式错误: {e}")
            
            if "end_date" in params and params["end_date"]:
                try:
                    # 验证格式
                    DateHelper.normalize_to_yyyy_mm_dd(params["end_date"])
                    # 标准化为 YYYYMMDD 格式（API 需要）
                    params["end_date"] = DateHelper.normalize_to_yyyymmdd(params["end_date"])
                except ValueError as e:
                    raise CollectorException(f"end_date 格式错误: {e}")
            
            if "ex_date" in params and params["ex_date"]:
                try:
                    # 验证格式
                    DateHelper.normalize_to_yyyy_mm_dd(params["ex_date"])
                    # 标准化为 YYYYMMDD 格式（API 需要）
                    params["ex_date"] = DateHelper.normalize_to_yyyymmdd(params["ex_date"])
                except ValueError as e:
                    raise CollectorException(f"ex_date 格式错误: {e}")
        
        # 验证日期范围
        if "start_date" in params and "end_date" in params:
            if params["start_date"] and params["end_date"]:
                start = DateHelper.parse_to_date(params["start_date"])
                end = DateHelper.parse_to_date(params["end_date"])
                if start > end:
                    raise CollectorException("start_date 不能大于 end_date")
        
        return True
    
    def _get_provider(self):
        """
        获取数据源提供者
        
        Returns:
            数据源提供者实例
            
        Raises:
            CollectorException: 如果无法创建或获取提供者
        """
        if self.provider is not None:
            return self.provider
        
        # 根据配置创建提供者
        if self.source == "tushare":
            try:
                return TushareProvider()
            except Exception as e:
                raise CollectorException(f"无法创建 TushareProvider: {e}")
        elif self.source == "akshare":
            # akshare 是本地库，不需要 Provider 实例
            # 直接在 Collector 中调用 akshare 函数
            return None
        else:
            raise CollectorException(f"不支持的数据源: {self.source}")

