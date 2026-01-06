"""
交易日历加载器

负责将处理后的交易日历数据持久化到数据库
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from loguru import logger

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException
from core.models.orm import TradeCalendarORM


class TradeCalendarLoader(BaseLoader):
    """
    交易日历加载器
    
    将转换后的交易日历数据加载到数据库表中
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化交易日历加载器
        
        Args:
            config: 配置字典，包含：
                - table: 表名（默认 "trade_calendar"）
                - batch_size: 批量大小（默认 1000）
                - upsert_keys: upsert 的键（默认 ['cal_date']）
                - preserve_null_columns: 保留NULL的列（默认所有 _open 列）
        """
        if config is None:
            config = {}
        if 'table' not in config:
            config['table'] = 'trade_calendar'
        if 'upsert_keys' not in config:
            config['upsert_keys'] = ['cal_date']
        if 'preserve_null_columns' not in config:
            # 默认保留所有 _open 列的现有值（如果新值为NULL）
            config['preserve_null_columns'] = [
                'sse_open', 'szse_open', 'cffex_open', 
                'shfe_open', 'czce_open', 'dce_open', 'ine_open'
            ]
        
        super().__init__(config)
    
    def _get_orm_model(self):
        """获取对应的ORM模型类"""
        if TradeCalendarORM is None:
            raise LoaderException("TradeCalendarORM 未导入，请检查依赖")
        return TradeCalendarORM
    
    def _get_required_columns(self) -> List[str]:
        """获取必需的数据列"""
        return ['cal_date']
    
    def load(self, data: pd.DataFrame, strategy: str) -> None:
        """
        加载交易日历数据到数据库
        
        Args:
            data: 待加载的交易日历数据 DataFrame
            strategy: 加载策略（append/replace/upsert）
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("交易日历数据为空，跳过加载")
            return
        
        logger.info(f"开始加载交易日历到表 {self.table}，数据量: {len(data)}")
        
        try:
            # 根据加载策略选择加载方式
            # 交易日历通常使用 upsert 策略，并且需要保留现有交易所的值
            if strategy == self.LOAD_STRATEGY_APPEND:
                self._load_append(data)
            elif strategy == self.LOAD_STRATEGY_REPLACE:
                self._load_replace(data)
            elif strategy == self.LOAD_STRATEGY_UPSERT:
                self._load_upsert(data)
            else:
                raise LoaderException(f"不支持的加载策略: {strategy}")
            
            logger.info(f"交易日历数据加载完成，表: {self.table}")
            
        except Exception as e:
            logger.error(f"加载交易日历数据失败: {e}")
            raise LoaderException(f"加载交易日历数据失败: {e}") from e
    
    def read(
        self,
        cal_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        从数据库读取交易日历数据
        
        Args:
            cal_date: 日历日期 (YYYY-MM-DD 或 YYYYMMDD)（可选，精确匹配）
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)（可选）
            
        Returns:
            pd.DataFrame: 交易日历数据
        """
        from utils.date_helper import DateHelper
        
        try:
            model_class = self._get_orm_model()
            
            with self._get_session() as session:
                query = session.query(model_class)
                
                # 构建过滤条件
                if cal_date is not None:
                    cal_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(cal_date)
                    cal_date_obj = DateHelper.parse_to_date(cal_date_normalized)
                    query = query.filter(model_class.cal_date == cal_date_obj)
                
                if start_date is not None:
                    start_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(start_date)
                    start_date_obj = DateHelper.parse_to_date(start_date_normalized)
                    query = query.filter(model_class.cal_date >= start_date_obj)
                
                if end_date is not None:
                    end_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(end_date)
                    end_date_obj = DateHelper.parse_to_date(end_date_normalized)
                    query = query.filter(model_class.cal_date <= end_date_obj)
                
                # 排序
                query = query.order_by(model_class.cal_date)
                
                results = query.all()
                
                if not results:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                data = []
                for row in results:
                    row_dict = {}
                    for column in model_class.__table__.columns:
                        value = getattr(row, column.name)
                        # 处理日期类型
                        if value is not None and hasattr(value, 'strftime'):
                            value = DateHelper.parse_to_str(value)
                        # 处理布尔类型
                        elif isinstance(value, bool):
                            value = 1 if value else 0
                        row_dict[column.name] = value
                    data.append(row_dict)
                
                df = pd.DataFrame(data)
                
                logger.debug(f"从数据库读取到 {len(df)} 条交易日历数据")
                return df
                
        except Exception as e:
            logger.error(f"读取交易日历数据失败: {e}")
            raise LoaderException(f"读取交易日历数据失败: {e}") from e

