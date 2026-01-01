"""
复权因子加载器

负责将处理后的复权因子数据持久化到数据库
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from loguru import logger

from core.loaders.base import BaseLoader
from core.common.exceptions import LoaderException
from core.models.orm import AdjFactorORM
from utils.date_helper import DateHelper


class AdjFactorLoader(BaseLoader):
    """
    复权因子加载器
    
    将转换后的复权因子数据加载到数据库表中
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化复权因子加载器
        
        Args:
            config: 配置字典，包含：
                - table: 表名（默认 "adj_factor"）
                - load_strategy: 加载策略（append/replace/upsert，默认 upsert）
                - batch_size: 批量大小（默认 1000）
                - upsert_keys: upsert 的键（默认 ['ts_code', 'trade_date']）
        """
        if config is None:
            config = {}
        if 'table' not in config:
            config['table'] = 'adj_factor'
        if 'upsert_keys' not in config:
            config['upsert_keys'] = ['ts_code', 'trade_date']
        
        super().__init__(config)
    
    def _get_orm_model(self):
        """获取对应的ORM模型类"""
        if AdjFactorORM is None:
            raise LoaderException("AdjFactorORM 未导入，请检查依赖")
        return AdjFactorORM
    
    def _get_required_columns(self) -> List[str]:
        """获取必需的数据列"""
        return ['ts_code', 'trade_date', 'adj_factor']
    
    def load(self, data: pd.DataFrame) -> None:
        """
        加载复权因子数据到数据库
        
        Args:
            data: 待加载的复权因子数据 DataFrame
            
        Raises:
            LoaderException: 加载失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("复权因子数据为空，跳过加载")
            return
        
        logger.info(f"开始加载复权因子数据到表 {self.table}，数据量: {len(data)}")
        
        try:
            # 根据加载策略选择加载方式
            if self.load_strategy == self.LOAD_STRATEGY_APPEND:
                self._load_append(data)
            elif self.load_strategy == self.LOAD_STRATEGY_REPLACE:
                self._load_replace(data)
            elif self.load_strategy == self.LOAD_STRATEGY_UPSERT:
                self._load_upsert(data)
            else:
                raise LoaderException(f"不支持的加载策略: {self.load_strategy}")
            
            logger.info(f"复权因子数据加载完成，表: {self.table}")
            
        except Exception as e:
            logger.error(f"加载复权因子数据失败: {e}")
            raise LoaderException(f"加载复权因子数据失败: {e}") from e
    
    def read(
        self,
        ts_codes: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """
        从数据库读取复权因子数据（读取所有历史除权除息日的复权因子）
        
        Args:
            ts_codes: 股票代码，可以是单个字符串或字符串列表（可选，如果不提供则读取所有股票）
            
        Returns:
            pd.DataFrame: 复权因子数据，包含所有历史除权除息日的数据
        """
        try:
            model_class = self._get_orm_model()
            
            with self._get_session() as session:
                query = session.query(model_class)
                
                # 构建过滤条件（只按股票代码过滤，不按日期过滤）
                if ts_codes is not None:
                    if isinstance(ts_codes, str):
                        ts_codes_list = [ts_codes]
                    else:
                        ts_codes_list = ts_codes
                    query = query.filter(model_class.ts_code.in_(ts_codes_list))
                
                # 排序（按股票代码和交易日期）
                query = query.order_by(model_class.ts_code, model_class.trade_date)
                
                results = query.all()
                
                if not results:
                    logger.info("数据库中未找到复权因子数据")
                    return pd.DataFrame()
                
                # 转换为DataFrame
                def _model_to_dict(row):
                    """将ORM模型实例转换为字典"""
                    from sqlalchemy import DECIMAL
                    result = {}
                    for column in row.__table__.columns:
                        value = getattr(row, column.name)
                        # 处理日期类型
                        if value is not None and hasattr(value, 'strftime'):
                            value = DateHelper.parse_to_str(value)
                        # 处理 DECIMAL 类型
                        elif isinstance(value, DECIMAL):
                            value = float(value)
                        result[column.name] = value
                    return result
                
                data = [_model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                # 转换trade_date为datetime（如果存在）
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                logger.info(f"从数据库读取到 {len(df)} 条复权因子数据（所有历史除权除息日）")
                return df
                
        except Exception as e:
            logger.error(f"读取复权因子数据失败: {e}")
            raise LoaderException(f"读取复权因子数据失败: {e}") from e

