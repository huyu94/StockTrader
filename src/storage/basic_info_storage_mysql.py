"""
股票基本信息存储管理器（MySQL版本）
"""
import pandas as pd
from typing import List, Optional
from loguru import logger
from .mysql_base import MySQLBaseStorage
from core.models.orm import Base, BasicInfoORM
from sqlalchemy.orm import Session
from utils.date_helper import DateHelper
import dotenv

dotenv.load_dotenv()


class BasicInfoStorageMySQL(MySQLBaseStorage):
    """股票基本信息存储管理器（MySQL版本）"""
    
    def __init__(self):
        super().__init__()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建所有表（如果不存在）
            Base.metadata.create_all(bind=self._get_engine())
            logger.debug("Basic info table initialized in MySQL")
        except Exception as e:
            logger.error(f"Failed to initialize basic info table: {e}")
            raise
    
    def load(self, 
            market: str = None,
            is_hs: str = None,
            exchange: str = None,
            industry: str = None,
            area: str = None,
            list_status: str = None,
            ) -> Optional[pd.DataFrame]:
        """读取股票基本信息数据"""
        try:
            with self._get_session() as session:
                query = session.query(BasicInfoORM)
                
                # 构建过滤条件
                if market is not None:
                    query = query.filter(BasicInfoORM.market == market)
                if is_hs is not None:
                    query = query.filter(BasicInfoORM.is_hs == is_hs)
                if exchange is not None:
                    query = query.filter(BasicInfoORM.exchange == exchange)
                if industry is not None:
                    query = query.filter(BasicInfoORM.industry == industry)
                if area is not None:
                    query = query.filter(BasicInfoORM.area == area)
                if list_status is not None:
                    query = query.filter(BasicInfoORM.list_status == list_status)
                
                # 排序
                query = query.order_by(BasicInfoORM.ts_code)
                
                # 执行查询并转换为DataFrame
                results = query.all()
                if not results:
                    return None
                
                # 转换为字典列表，再转为DataFrame
                data = [self._model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                return df
        except Exception as e:
            logger.error(f"Failed to load basic info: {e}")
            return None
    
    def _model_to_dict(self, model_instance) -> dict:
        """将ORM模型实例转换为字典"""
        result = {}
        for column in model_instance.__table__.columns:
            value = getattr(model_instance, column.name)
            # 处理日期类型：使用DateHelper统一转换为YYYYMMDD格式
            if value is not None and hasattr(value, 'strftime'):
                value = DateHelper.parse_to_str(value)
            result[column.name] = value
        return result
    
    def check_update_needed(self) -> bool:
        """判断是否需要更新数据"""
        try:
            with self._get_session() as session:
                # 检查是否有数据
                count = session.query(BasicInfoORM).count()
                
                if count == 0:
                    logger.debug("Update needed for basic info: no data in database")
                    return True
                
                # 检查最新更新时间
                max_date_obj = session.query(BasicInfoORM.updated_at).order_by(
                    BasicInfoORM.updated_at.desc()
                ).first()
                
                if not max_date_obj or not max_date_obj[0]:
                    return True
                
                max_date = max_date_obj[0]
                # 将日期转换为YYYYMMDD格式进行比较（使用DateHelper）
                if max_date is not None:
                    max_date_str = DateHelper.parse_to_str(max_date)
                    today_str = DateHelper.today()
                    
                    if max_date_str < today_str:
                        logger.debug(f"Update needed for basic info: cache expired ({max_date_str} < {today_str})")
                        return True
                
                return False
        except Exception as e:
            logger.warning(f"Error checking update for basic info: {e}")
            return True
    
    def write(self, df: pd.DataFrame):
        """
        写入数据
        
        流程：
        1. 确保表结构包含所有需要的列
        2. 添加更新时间
        3. 使用批量UPSERT写入
        """
        if df is None or df.empty:
            return
        
        try:
            # 添加更新时间（YYYYMMDD格式）
            df_copy = df.copy()
            today = DateHelper.today()
            df_copy["updated_at"] = today
            
            # 确保ts_code存在
            if "ts_code" not in df_copy.columns:
                logger.error("DataFrame must have 'ts_code' column")
                return
            
            # 获取表中实际存在的列（通过ORM模型）
            model_columns = {col.name for col in BasicInfoORM.__table__.columns}
            available_columns = [col for col in df_copy.columns if col in model_columns]
            
            if not available_columns:
                logger.error("No matching columns found between DataFrame and table")
                return
            
            df_to_write = df_copy[available_columns].copy()
            
            # 使用批量UPSERT写入
            with self._get_session() as session:
                self._bulk_upsert_dataframe(session, BasicInfoORM, df_to_write)
            
            logger.info(f"Basic info saved: {len(df_to_write)} records")
        except Exception as e:
            logger.error(f"Failed to write basic info: {e}")
            raise

    def get_all_ts_codes(self) -> List[str]:
        """获取数据库中所有股票代码"""
        try:
            with self._get_session() as session:
                query = session.query(BasicInfoORM.ts_code).distinct()
                results = query.all()
                ts_codes = [row[0] for row in results]
                return ts_codes
        except Exception as e:
            logger.error(f"Failed to get all ts codes: {e}")
            return []