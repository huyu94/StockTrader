"""
分时K线数据采集器

负责从数据源采集分时K线数据
使用 akshare 的实时行情接口获取数据
"""

from typing import Any, Dict
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
    
    def collect(self, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        采集分时K线数据（使用 akshare 实时行情接口）
        
        Args:
            params: 采集参数（可选，当前版本暂不使用）
                
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
        if params is None:
            params = {}
        
        logger.info(f"开始采集实时行情数据（akshare）")
        
        # 使用 akshare 获取实时行情数据（只返回原始数据，不做任何处理）
        try:
            df = self._retry_collect(
                self._fetch_akshare_spot_data
            )
            
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
        try:
            # 调用 akshare 的实时行情接口
            df = ak.stock_zh_a_spot_em()
            
            if df is None or df.empty:
                logger.warning("akshare 返回空数据")
                return pd.DataFrame()
            
            logger.debug(f"akshare 返回 {len(df)} 条实时行情数据")
            return df
            
        except ImportError:
            raise CollectorException("akshare 库未安装，请使用 'pip install akshare' 安装")
        except Exception as e:
            logger.error(f"调用 akshare 接口失败: {e}")
            raise CollectorException(f"调用 akshare 接口失败: {e}") from e
    

