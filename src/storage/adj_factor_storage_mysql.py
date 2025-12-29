"""
复权因子存储管理器（MySQL版本）
"""
import pandas as pd
from typing import List, Optional
from loguru import logger
from .mysql_base import MySQLBaseStorage
from .orm_models import Base, AdjFactorORM
from utils.date_helper import DateHelper
import dotenv

dotenv.load_dotenv()


class AdjFactorStorageMySQL(MySQLBaseStorage):
    """复权因子存储管理器（MySQL版本）"""
    
    def __init__(self):
        super().__init__()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建所有表（如果不存在）
            Base.metadata.create_all(bind=self._get_engine())
            logger.debug("Adj factor table initialized in MySQL")
        except Exception as e:
            logger.error(f"Failed to initialize adj factor table: {e}")
            raise
    
    def load(self, 
            ts_code: Optional[str] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
            ) -> Optional[pd.DataFrame]:
        """
        读取复权因子数据
        
        Args:
            ts_code: 股票代码（可选）
            start_date: 开始日期（可选，YYYY-MM-DD 或 YYYYMMDD）
            end_date: 结束日期（可选，YYYY-MM-DD 或 YYYYMMDD）
            
        Returns:
            包含复权因子数据的 DataFrame，如果无数据或出错则返回 None
        """
        try:
            with self._get_session() as session:
                query = session.query(AdjFactorORM)
                
                # 构建过滤条件
                if ts_code is not None:
                    query = query.filter(AdjFactorORM.ts_code == ts_code)
                
                if start_date is not None:
                    # 标准化日期格式
                    start_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(start_date)
                    start_date_obj = DateHelper.parse_to_date(start_date_normalized)
                    query = query.filter(AdjFactorORM.trade_date >= start_date_obj)
                
                if end_date is not None:
                    # 标准化日期格式
                    end_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(end_date)
                    end_date_obj = DateHelper.parse_to_date(end_date_normalized)
                    query = query.filter(AdjFactorORM.trade_date <= end_date_obj)
                
                # 排序
                query = query.order_by(AdjFactorORM.ts_code, AdjFactorORM.trade_date)
                
                # 执行查询并转换为DataFrame
                results = query.all()
                if not results:
                    return None
                
                # 转换为字典列表，再转为DataFrame
                data = [self._model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                return df
        except Exception as e:
            logger.error(f"Failed to load adj factor: {e}")
            return None
    
    def _model_to_dict(self, model_instance) -> dict:
        """将ORM模型实例转换为字典"""
        result = {}
        for column in model_instance.__table__.columns:
            value = getattr(model_instance, column.name)
            # 处理日期类型：使用DateHelper统一转换为YYYY-MM-DD格式
            if value is not None and hasattr(value, 'strftime'):
                value = DateHelper.parse_to_str(value)
            result[column.name] = value
        return result
    
    def write(self, df: pd.DataFrame, show_progress: bool = True):
        """
        写入复权因子数据
        
        流程：
        1. 确保表结构包含所有需要的列
        2. 处理日期格式
        3. 使用批量UPSERT写入（带进度条）
        
        Args:
            df: 复权因子数据 DataFrame，必须包含 ts_code, ex_date, adj_factor 列
            show_progress: 是否显示进度条（默认True）
        """
        if df is None or df.empty:
            logger.warning("DataFrame is empty, nothing to write")
            return
        
        try:
            logger.info(f"开始写入复权因子数据，共 {len(df)} 条记录...")
            
            df_copy = df.copy()
            
            # 获取表中实际存在的列（通过ORM模型）
            model_columns = {col.name for col in AdjFactorORM.__table__.columns}
            available_columns = [col for col in df_copy.columns if col in model_columns]
            
            if not available_columns:
                logger.error("No matching columns found between DataFrame and table")
                return
            
            df_to_write = df_copy[available_columns].copy()
            
            # 使用批量UPSERT写入（内部会显示进度条）
            with self._get_session() as session:
                inserted_count = self._bulk_upsert_dataframe(session, AdjFactorORM, df_to_write, show_progress=show_progress)
            
            logger.info(f"✓ 复权因子数据写入成功，共写入 {inserted_count} 条记录")
        except Exception as e:
            logger.error(f"Failed to write adj factor: {e}")
            raise

    
    def get_ex_stock_codes(self, start_date: str, end_date: str) -> List[str]:
        """
        获取除权除息股票代码
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        df = self.load(start_date=start_date, end_date=end_date)
        return df['ts_code'].unique().tolist()
