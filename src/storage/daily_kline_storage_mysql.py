"""
日线行情存储管理器（MySQL版本）
使用MySQL数据库和SQLAlchemy ORM，支持批量写入
"""
import pandas as pd
from typing import Optional, List
from loguru import logger
from .mysql_base import MySQLBaseStorage
from .orm_models import Base, DailyKline
from src.utils.date_helper import DateHelper
import dotenv

dotenv.load_dotenv()


class DailyKlineStorageMySQL(MySQLBaseStorage):
    """日线行情存储管理器（MySQL版本）
    使用MySQL数据库，支持更好的并发和性能
    
    优势：
    1. 支持更高的并发连接
    2. 更好的事务处理
    3. 支持连接池管理
    4. 支持UPSERT（INSERT ... ON DUPLICATE KEY UPDATE），自动去重
    """
    
    def __init__(self):
        super().__init__()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建所有表（如果不存在）
            Base.metadata.create_all(bind=self._get_engine())
            logger.debug("Daily kline table initialized in MySQL")
        except Exception as e:
            logger.error(f"Failed to initialize daily kline table: {e}")
            raise
    
    def load(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """读取单只股票的日线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期，支持 YYYYMMDD 或 YYYY-MM-DD 格式
            end_date: 结束日期，支持 YYYYMMDD 或 YYYY-MM-DD 格式
            
        Returns:
            包含日线数据的 DataFrame，如果无数据或出错则返回空 DataFrame
        """
        try:
            # 参数验证
            if not ts_code:
                logger.warning("ts_code is empty")
                return pd.DataFrame()
            
            # 统一日期格式为 YYYY-MM-DD（MySQL DATE类型）
            try:
                start_date_normalized = DateHelper.normalize_str_to_str(start_date)
                end_date_normalized = DateHelper.normalize_str_to_str(end_date)
                # 转换为 YYYY-MM-DD 格式用于MySQL查询
                start_date_normalized = DateHelper.to_display(start_date_normalized)
                end_date_normalized = DateHelper.to_display(end_date_normalized)
            except Exception as e:
                logger.error(f"Invalid date format for {ts_code}: start={start_date}, end={end_date}, error={e}")
                return pd.DataFrame()
            
            with self._get_session() as session:
                query = session.query(DailyKline).filter(
                    DailyKline.ts_code == ts_code,
                    DailyKline.trade_date >= start_date_normalized,
                    DailyKline.trade_date <= end_date_normalized
                ).order_by(DailyKline.trade_date)
                
                results = query.all()
                
                if not results:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                data = [self._model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                # 转换trade_date为datetime
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                return df
        except Exception as e:
            logger.error(f"Failed to load daily kline for {ts_code}: {e}")
            return pd.DataFrame()
    
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
    
    def write(self, df: pd.DataFrame) -> bool:
        """
        写入股票数据（统一接口，支持批量写入）
        
        注意：
        - 假设传入的 DataFrame 已经是正确格式，不做额外验证
        - 数据验证应该在外层使用 Pydantic 模型完成
        - DataFrame 必须包含 ts_code 列
        - 支持单只股票或多只股票的数据写入
        - 使用 UPSERT 自动去重
        
        :param df: 股票数据 DataFrame，必须包含 ts_code 和 trade_date 列
        :return: True 表示成功，False 表示失败
        """
        if df.empty:
            return True
        
        try:
            # 确保日期格式正确（YYYY-MM-DD用于MySQL存储）
            df_copy = df.copy()
            if "trade_date" in df_copy.columns:
                # 统一日期格式：使用DateHelper处理，然后转换为YYYY-MM-DD
                if df_copy["trade_date"].dtype == 'object':
                    def normalize_date(d):
                        try:
                            if pd.isna(d):
                                return None
                            # 使用DateHelper统一处理，然后转换为YYYY-MM-DD
                            normalized = DateHelper.parse_to_str(str(d))
                            return DateHelper.to_display(normalized)
                        except:
                            return None
                    df_copy["trade_date"] = df_copy["trade_date"].apply(normalize_date)
                elif pd.api.types.is_datetime64_any_dtype(df_copy["trade_date"]):
                    # 如果是datetime类型，使用DateHelper处理
                    df_copy["trade_date"] = df_copy["trade_date"].apply(
                        lambda d: DateHelper.to_display(DateHelper.parse_to_str(d)) if pd.notna(d) else None
                    )
            
            # 获取表中实际存在的列
            model_columns = {col.name for col in DailyKline.__table__.columns}
            available_columns = [col for col in df_copy.columns if col in model_columns]
            
            if not available_columns:
                logger.error("No matching columns found between DataFrame and table")
                return False
            
            df_to_write = df_copy[available_columns].copy()
            
            # 使用批量UPSERT写入
            with self._get_session() as session:
                self._bulk_upsert_dataframe(session, DailyKline, df_to_write)
            
            logger.debug(f"✓ Write succeeded ({len(df_to_write)} rows)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to write data: {e}")
            return False
    
    def load_multiple(self, ts_codes: List[str]) -> pd.DataFrame:
        """批量读取多只股票的数据"""
        try:
            if not ts_codes:
                return pd.DataFrame()
            
            with self._get_session() as session:
                query = session.query(DailyKline).filter(
                    DailyKline.ts_code.in_(ts_codes)
                ).order_by(DailyKline.ts_code, DailyKline.trade_date)
                
                results = query.all()
                
                if not results:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                data = [self._model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                # 转换trade_date为datetime
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                return df
        except Exception as e:
            logger.error(f"Failed to load multiple stocks: {e}")
            return pd.DataFrame()
    
    def get_stock_count(self) -> int:
        """获取数据库中股票数量"""
        try:
            with self._get_session() as session:
                # 使用distinct查询
                count = session.query(DailyKline.ts_code).distinct().count()
                return count
        except Exception as e:
            logger.error(f"Failed to get stock count: {e}")
            return 0
    
    def get_total_rows(self) -> int:
        """获取数据库总行数"""
        try:
            with self._get_session() as session:
                count = session.query(DailyKline).count()
                return count
        except Exception as e:
            logger.error(f"Failed to get total rows: {e}")
            return 0

