"""
交易日历存储管理器（MySQL版本）
"""
import pandas as pd
from typing import Optional
from loguru import logger
from .mysql_base import MySQLBaseStorage
from core.models.orm import Base, TradeCalendarORM
from utils.date_helper import DateHelper
from src.fetch.calendar.calendar_model import TradeCalendar
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
                    TradeCalendarORM.cal_date,
                    TradeCalendarORM.is_open
                ).filter(
                    TradeCalendarORM.exchange == exchange
                ).order_by(TradeCalendarORM.trade_date)
                
                results = query.all()
                
                if not results:
                    return None
                
                # 转换为DataFrame，将 trade_date 列重命名为 cal_date 以保持与外部接口一致
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
                count = session.query(TradeCalendarORM).filter(
                    TradeCalendarORM.exchange == exchange
                ).count()
                
                if count == 0:
                    logger.debug(f"Update needed for {exchange}: no data in database")
                    return True
                
                # 检查最新日期
                max_date_obj = session.query(TradeCalendarORM.trade_date).filter(
                    TradeCalendarORM.exchange == exchange
                ).order_by(TradeCalendarORM.trade_date.desc()).first()
                
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
            
            return True


    def write_df(self, df: pd.DataFrame):
        """写入数据（SQL层面智能合并：不会覆盖已有交易所的数据）"""
        if df is None or df.empty:
            return
        
        try:
            df_copy = df.copy()

            validated_df, failed_records = TradeCalendar.validate_dataframe(df_copy)
            
            if failed_records:
                logger.warning(f"验证过程中存在{len(failed_records)}条数据验证失败")
                for failed_record in failed_records[:5]:  # 只显示前5条错误
                    logger.warning(f"失败数据: {failed_record['data']}, 错误: {failed_record['error']}")
            
            if validated_df.empty:
                logger.warning("验证后没有有效数据")
                return
            
            # 选择需要的列（匹配ORM模型）
            model_columns = {col.name for col in TradeCalendarORM.__table__.columns}
            available_columns = [col for col in validated_df.columns if col in model_columns]
            df_to_write = validated_df[available_columns].copy()
            
            
            # 获取所有需要保留NULL的列（所有 _open 列）
            preserve_null_columns = [col for col in df_to_write.columns if col.endswith('_open')]
        
            logger.debug(f"Writing calendar data with columns: {list(df_to_write.columns)}, shape: {df_to_write.shape}")

            # 使用批量UPSERT写入，传入 _open 列以保留现有值
            with self._get_session() as session:
                self._bulk_upsert_dataframe(
                    session, 
                    TradeCalendarORM, 
                    df_to_write,
                    preserve_null_columns=preserve_null_columns
                )
            
            logger.info(f"Trade calendar saved: {len(validated_df)} records")
        except Exception as e:
            logger.error(f"Failed to write calendar: {e}")
            raise

