"""
交易日历存储管理器（SQLite版本）
"""
import pandas as pd
from datetime import datetime
from typing import Optional
from loguru import logger
from .sqlite_base import SQLiteBaseStorage
from .sql_models import TRADE_CALENDAR_TABLE, TRADE_CALENDAR_INDEXES
import dotenv

dotenv.load_dotenv()


class CalendarStorageSQLite(SQLiteBaseStorage):
    """交易日历存储管理器（SQLite版本）"""
    
    def __init__(self, db_name: str = "stock_data.db"):
        super().__init__(db_name)
    
    def _init_database(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            # 使用 SQL 模型定义创建表
            conn.execute(TRADE_CALENDAR_TABLE)
            
            # 创建索引
            for index_sql in TRADE_CALENDAR_INDEXES:
                conn.execute(index_sql)
            
            conn.commit()
            logger.debug(f"Trade calendar table initialized in {self.db_path}")
    
    def load(self, exchange: str) -> Optional[pd.DataFrame]:
        """读取指定交易所的交易日历数据"""
        try:
            with self._get_connection() as conn:
                query = """
                    SELECT cal_date, is_open 
                    FROM trade_calendar 
                    WHERE exchange = ? 
                    ORDER BY cal_date
                """
                df = pd.read_sql_query(query, conn, params=(exchange,))
                
                if df.empty:
                    return None
                
                return df
        except Exception as e:
            logger.error(f"Failed to load calendar for {exchange}: {e}")
            return None
    
    def check_update_needed(self, exchange: str) -> bool:
        """判断是否需要更新数据"""
        try:
            with self._get_connection() as conn:
                # 检查是否有数据
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM trade_calendar WHERE exchange = ?",
                    (exchange,)
                )
                count = cursor.fetchone()[0]
                
                if count == 0:
                    logger.debug(f"Update needed for {exchange}: no data in database")
                    return True
                
                # 检查最新日期
                cursor = conn.execute(
                    "SELECT MAX(cal_date) FROM trade_calendar WHERE exchange = ?",
                    (exchange,)
                )
                max_date_str = cursor.fetchone()[0]
                
                if not max_date_str:
                    return True
                
                try:
                    max_date = datetime.strptime(max_date_str, "%Y%m%d").date()
                    today = datetime.now().date()
                    
                    if max_date < today:
                        logger.debug(f"Update needed for {exchange}: cache expired ({max_date} < {today})")
                        return True
                except ValueError:
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
            
            # 统一 cal_date 格式为 DATE 类型（YYYY-MM-DD）
            if "cal_date" in df_copy.columns:
                if pd.api.types.is_datetime64_any_dtype(df_copy["cal_date"]):
                    df_copy["cal_date"] = df_copy["cal_date"].dt.strftime("%Y-%m-%d")
                else:
                    # 如果是 YYYYMMDD 格式，转换为 YYYY-MM-DD
                    df_copy["cal_date"] = df_copy["cal_date"].astype(str)
                    df_copy["cal_date"] = df_copy["cal_date"].apply(
                        lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]}" if len(x) == 8 and x.isdigit() else x
                    )
            
            # 确保is_open是整数
            if "is_open" in df_copy.columns:
                df_copy["is_open"] = df_copy["is_open"].astype(int)
            
            # 选择需要的列
            required_columns = ["exchange", "cal_date", "is_open"]
            available_columns = [col for col in required_columns if col in df_copy.columns]
            df_to_write = df_copy[available_columns].copy()
            
            with self._get_connection() as conn:
                # 使用 INSERT OR REPLACE 实现覆盖写入
                df_to_write.to_sql(
                    "trade_calendar",
                    conn,
                    if_exists="append",
                    index=False,
                    method=SQLiteBaseStorage._upsert_method
                )
            
            logger.info(f"Trade calendar saved for {exchange}: {len(df_to_write)} records")
        except Exception as e:
            logger.error(f"Failed to write calendar for {exchange}: {e}")

