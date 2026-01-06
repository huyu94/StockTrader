"""
日K线数据加载器

负责将处理后的日K线数据持久化到数据库
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from loguru import logger

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException
from core.models.orm import DailyKlineORM
from utils.date_helper import DateHelper



class DailyKlineLoader(BaseLoader):
    """
    日K线数据加载器
    
    将转换后的日K线数据加载到数据库表中
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化日K线数据加载器
        
        Args:
            config: 配置字典，包含：
                - table: 表名（默认 "daily_kline"）
                - batch_size: 批量大小（默认 1000）
                - upsert_keys: upsert 的键（默认 ['ts_code', 'trade_date']）
        """
        if config is None:
            config = {}
        if 'table' not in config:
            config['table'] = 'daily_kline'
        if 'upsert_keys' not in config:
            config['upsert_keys'] = ['ts_code', 'trade_date']
        
        super().__init__(config)
    
    def _get_orm_model(self):
        """获取对应的ORM模型类"""
        if DailyKlineORM is None:
            raise LoaderException("DailyKlineORM 未导入，请检查依赖")
        return DailyKlineORM
    
    def _get_required_columns(self) -> List[str]:
        """
        获取必需的数据列
        
        注意：对于只更新前复权价格的场景，只需要主键列和前复权价格列
        """
        # 如果配置了只更新前复权价格，则只要求主键列
        if self.config.get("update_qfq_only", False):
            return ['ts_code', 'trade_date']
        return ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
    
    def load(self, data: pd.DataFrame, strategy: str) -> None:
        """
        加载日K线数据到数据库
        
        Args:
            data: 待加载的日K线数据 DataFrame
            strategy: 加载策略（append/replace/upsert）
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("日K线数据为空，跳过加载")
            return
        
        try:
            # 根据加载策略选择加载方式
            if strategy == self.LOAD_STRATEGY_APPEND:
                self._load_append(data)
            elif strategy == self.LOAD_STRATEGY_REPLACE:
                self._load_replace(data)
            elif strategy == self.LOAD_STRATEGY_UPSERT:
                self._load_upsert(data)
            else:
                raise LoaderException(f"不支持的加载策略: {strategy}")
            
        except Exception as e:
            logger.error(f"加载日K线数据失败: {e}")
            raise LoaderException(f"加载日K线数据失败: {e}") from e
    
    def read(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        从数据库读取日K线数据
        
        Args:
            ts_code: 股票代码，可以是单个字符串（可选，如果不提供则读取所有股票）
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            
        Returns:
            pd.DataFrame: 日K线数据
        """
        try:
            model_class = self._get_orm_model()
            
            with self._get_session() as session:
                query = session.query(model_class)
                
                # 构建过滤条件
                if ts_code is not None:
                    query = query.filter(model_class.ts_code == ts_code)
                
                if start_date is not None:
                    start_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(start_date)
                    start_date_obj = DateHelper.parse_to_date(start_date_normalized)
                    query = query.filter(model_class.trade_date >= start_date_obj)
                
                if end_date is not None:
                    end_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(end_date)
                    end_date_obj = DateHelper.parse_to_date(end_date_normalized)
                    query = query.filter(model_class.trade_date <= end_date_obj)
                
                # 排序
                query = query.order_by(model_class.trade_date)
                
                results = query.all()
                
                if not results:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                data = [DailyKlineORM._model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                # 转换trade_date为datetime（如果存在）
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                return df
                
        except Exception as e:
            logger.error(f"读取日K线数据失败: {e}")
            raise LoaderException(f"读取日K线数据失败: {e}") from e



