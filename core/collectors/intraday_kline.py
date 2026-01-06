"""
分时K线数据采集器

负责从数据源采集分时K线数据
使用 akshare 的实时行情接口获取数据
"""

from typing import Any, Dict
import time
import pandas as pd
from loguru import logger

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException
import akshare as ak


class IntradayKlineCollector(BaseCollector):
    """
    分时K线数据采集器
    
    从数据源（Tushare、Akshare等）采集股票的分时K线数据
    支持按股票代码和日期采集
    """
    
    def collect(self) -> pd.DataFrame:
        """
        采集分时K线数据（使用 akshare 实时行情接口）
        
        Args:
                
        Returns:
            pd.DataFrame: akshare 返回的原始数据，包含 akshare 原始列名
                - 代码: 股票代码
                - 最新价: 最新价
                - 成交量: 成交量
                - 成交额: 成交额
                - 其他 akshare 返回的列
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        
        logger.info(f"开始采集实时行情数据（akshare）")
        
        # 使用 akshare 获取实时行情数据（只返回原始数据，不做任何处理）
        # 注意：_fetch_akshare_spot_data 方法内部已实现重试机制
        try:
            df = self._fetch_akshare_spot_data()
            
            if df is None or df.empty:
                logger.warning("未采集到任何数据")
                return pd.DataFrame()
            
            logger.info(f"采集完成，共 {len(df)} 条原始数据记录")
            return df
            
        except Exception as e:
            logger.error(f"采集实时行情数据失败: {e}")
            raise CollectorException(f"采集实时行情数据失败: {e}") from e
    
    def _fetch_akshare_spot_data(self) -> pd.DataFrame:
        """
        使用 akshare 获取实时行情数据
        
        Returns:
            pd.DataFrame: akshare 返回的原始数据
        """
        max_retries = 3
        retry_delay = 2  # 秒
        
        for attempt in range(max_retries):
            start_time = time.time()
            try:
                # 调用 akshare 的实时行情接口
                df = ak.stock_zh_a_spot_em()
                elapsed = time.time() - start_time
                
                if df is None or df.empty:
                    logger.warning("akshare 返回空数据")
                    return pd.DataFrame()
                
                logger.debug(f"akshare 返回 {len(df)} 条实时行情数据，耗时: {elapsed:.3f}s")
                return df
                
            except ImportError:
                raise CollectorException("akshare 库未安装，请使用 'pip install akshare' 安装")
            except Exception as e:
                elapsed = time.time() - start_time
                
                if attempt < max_retries - 1:
                    logger.warning(f"调用 akshare 接口失败 (尝试 {attempt + 1}/{max_retries})，"
                                 f"耗时: {elapsed:.3f}s，错误: {e}。{retry_delay}秒后重试...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"调用 akshare 接口失败，已重试 {max_retries} 次，"
                               f"耗时: {elapsed:.3f}s，错误: {e}")
                    raise CollectorException(f"调用 akshare 接口失败: {e}") from e
    

