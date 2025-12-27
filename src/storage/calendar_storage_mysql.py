"""
交易日历存储管理器（MySQL版本）
"""
import pandas as pd
from typing import Optional
from loguru import logger
from .mysql_base import MySQLBaseStorage
from .orm_models import Base, TradeCalendar
from src.utils.date_helper import DateHelper
import dotenv

dotenv.load_dotenv()


class CalendarStorageMySQL(MySQLBaseStorage):
    """交易日历存储管理器（MySQL版本）"""
    
    def __init__(self):
        super().__init__()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建所有表（如果不存在）
            Base.metadata.create_all(bind=self._get_engine())
            logger.debug("Trade calendar table initialized in MySQL")
        except Exception as e:
            logger.error(f"Failed to initialize trade calendar table: {e}")
            raise
    
    def load(self, exchange: str) -> Optional[pd.DataFrame]:
        """读取指定交易所的交易日历数据"""
        try:
            with self._get_session() as session:
                query = session.query(
                    TradeCalendar.cal_date,
                    TradeCalendar.is_open
                ).filter(
                    TradeCalendar.exchange == exchange
                ).order_by(TradeCalendar.cal_date)
                
                results = query.all()
                
                if not results:
                    return None
                
                # 转换为DataFrame
                data = [{"cal_date": row.cal_date, "is_open": row.is_open} for row in results]
                df = pd.DataFrame(data)
                
                return df
        except Exception as e:
            logger.error(f"Failed to load calendar for {exchange}: {e}")
            return None
    
    def check_update_needed(self, exchange: str) -> bool:
        """判断是否需要更新数据"""
        try:
            with self._get_session() as session:
                # 检查是否有数据
                count = session.query(TradeCalendar).filter(
                    TradeCalendar.exchange == exchange
                ).count()
                
                if count == 0:
                    logger.debug(f"Update needed for {exchange}: no data in database")
                    return True
                
                # 检查最新日期
                max_date_obj = session.query(TradeCalendar.cal_date).filter(
                    TradeCalendar.exchange == exchange
                ).order_by(TradeCalendar.cal_date.desc()).first()
                
                if not max_date_obj or not max_date_obj[0]:
                    return True
                
                max_date = max_date_obj[0]
                # 将日期转换为YYYYMMDD格式进行比较（使用DateHelper）
                if max_date is not None:
                    max_date_str = DateHelper.parse_to_str(max_date)
                    today_str = DateHelper.today()
                    
                    if max_date_str < today_str:
                        logger.debug(f"Update needed for {exchange}: cache expired ({max_date_str} < {today_str})")
                        return True
                
                return False
        except Exception as e:
            logger.warning(f"Error checking update for {exchange}: {e}")
            return True
    
    def write(self, df: pd.DataFrame, exchange: str):
        """写入数据"""
        if df is None or df.empty:
            return
        
        try:
            # 确保有exchange列
            df_copy = df.copy()
            if "exchange" not in df_copy.columns:
                df_copy["exchange"] = exchange
            
            # 统一 cal_date 格式为 DATE 类型（YYYY-MM-DD用于MySQL存储）
            if "cal_date" in df_copy.columns:
                if pd.api.types.is_datetime64_any_dtype(df_copy["cal_date"]):
                    # 如果是datetime类型，使用DateHelper处理
                    df_copy["cal_date"] = df_copy["cal_date"].apply(
                        lambda d: DateHelper.to_display(DateHelper.parse_to_str(d)) if pd.notna(d) else None
                    )
                else:
                    # 使用DateHelper统一处理日期格式
                    def normalize_cal_date(d):
                        try:
                            if pd.isna(d):
                                return None
                            # 使用DateHelper统一处理，然后转换为YYYY-MM-DD
                            normalized = DateHelper.parse_to_str(str(d))
                            return DateHelper.to_display(normalized)
                        except:
                            return None
                    df_copy["cal_date"] = df_copy["cal_date"].astype(str)
                    df_copy["cal_date"] = df_copy["cal_date"].apply(normalize_cal_date)
            
            # 确保is_open是整数
            if "is_open" in df_copy.columns:
                df_copy["is_open"] = df_copy["is_open"].astype(int)
            
            # 选择需要的列
            model_columns = {col.name for col in TradeCalendar.__table__.columns}
            available_columns = [col for col in df_copy.columns if col in model_columns]
            df_to_write = df_copy[available_columns].copy()
            
            # 使用批量UPSERT写入
            with self._get_session() as session:
                self._bulk_upsert_dataframe(session, TradeCalendar, df_to_write)
            
            logger.info(f"Trade calendar saved for {exchange}: {len(df_to_write)} records")
        except Exception as e:
            logger.error(f"Failed to write calendar for {exchange}: {e}")
            raise

