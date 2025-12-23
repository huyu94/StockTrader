"""
股票基本信息存储管理器（SQLite版本）
"""
import pandas as pd
from datetime import datetime
from typing import Optional
from loguru import logger
from .sqlite_base import SQLiteBaseStorage
from .sql_models import BASIC_INFO_TABLE, BASIC_INFO_INDEXES
import dotenv

dotenv.load_dotenv()


class BasicInfoStorageSQLite(SQLiteBaseStorage):
    """股票基本信息存储管理器（SQLite版本）"""
    
    def __init__(self, db_name: str = "stock_data.db"):
        super().__init__(db_name)
    
    def _init_database(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            # 使用 SQL 模型定义创建表
            conn.execute(BASIC_INFO_TABLE)
            
            # 如果表已存在但没有新列，则添加这些列
            try:
                cursor = conn.execute("PRAGMA table_info(basic_info)")
                columns = [row[1] for row in cursor.fetchall()]
                
                if "list_status" not in columns:
                    conn.execute("ALTER TABLE basic_info ADD COLUMN list_status TEXT")
                if "is_hs" not in columns:
                    conn.execute("ALTER TABLE basic_info ADD COLUMN is_hs TEXT")
                if "exchange" not in columns:
                    conn.execute("ALTER TABLE basic_info ADD COLUMN exchange TEXT")
                    
                conn.commit()
                logger.debug("Added missing columns to basic_info table if needed")
            except Exception as e:
                logger.debug(f"Could not add columns to basic_info table (may already exist): {e}")
            
            # 创建索引
            for index_sql in BASIC_INFO_INDEXES:
                conn.execute(index_sql)
            
            conn.commit()
            logger.debug(f"Basic info table initialized in {self.db_path}")
    
    def load(self, 
            market: str = None,
            is_hs: str = None,
            exchange: str = None,
            industry: str = None,
            area: str = None,
            list_status: str = None,
            ) -> Optional[pd.DataFrame]:
        """读取股票基本信息数据"""
        filters = {
            "market": market,
            "is_hs": is_hs,
            "exchange": exchange,
            "industry": industry,
            "area": area,
            "list_status": list_status,
        }
        conditions = [f"{k} = ?" for k, v in filters.items() if v is not None]
        params = [v for v in filters.values() if v is not None]
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT * FROM basic_info {where_clause} ORDER BY ts_code"
        
        try:
            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                if df.empty:
                    return None
                
                return df
        except Exception as e:
            logger.error(f"Failed to load basic info: {e}")
            return None
    
    def check_update_needed(self) -> bool:
        """判断是否需要更新数据"""
        try:
            with self._get_connection() as conn:
                # 检查是否有数据
                cursor = conn.execute("SELECT COUNT(*) FROM basic_info")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    logger.debug("Update needed for basic info: no data in database")
                    return True
                
                # 检查最新更新时间
                cursor = conn.execute("SELECT MAX(updated_at) FROM basic_info")
                max_date_str = cursor.fetchone()[0]
                
                if not max_date_str:
                    return True
                
                try:
                    max_date = datetime.strptime(max_date_str, "%Y-%m-%d").date()
                    today = datetime.now().date()
                    
                    if max_date < today:
                        logger.debug(f"Update needed for basic info: cache expired ({max_date} < {today})")
                        return True
                except ValueError:
                    return True
                
                return False
        except Exception as e:
            logger.warning(f"Error checking update for basic info: {e}")
            return True
    
    def write(self, df: pd.DataFrame):
        """
        写入数据
        
        流程：
        1. 确保表结构包含所有需要的列（自动添加缺失列）
        2. 只选择表中存在的列进行写入
        3. 使用 INSERT OR REPLACE 实现覆盖写入
        """
        if df is None or df.empty:
            return
        
        try:
            # 确保表结构是最新的（添加缺失的列）
            self._ensure_table_columns()
            
            # 添加更新时间
            df_copy = df.copy()
            today = datetime.now().strftime("%Y-%m-%d")
            df_copy["updated_at"] = today
            
            # 确保ts_code是主键
            if "ts_code" not in df_copy.columns:
                logger.error("DataFrame must have 'ts_code' column")
                return
            
            # 获取表中实际存在的列
            with self._get_connection() as conn:
                cursor = conn.execute("PRAGMA table_info(basic_info)")
                table_columns = [row[1] for row in cursor.fetchall()]
                
                # 只选择表中存在的列
                available_columns = [col for col in df_copy.columns if col in table_columns]
                if not available_columns:
                    logger.error("No matching columns found between DataFrame and table")
                    return
                
                df_to_write = df_copy[available_columns].copy()
                
                # 使用 INSERT OR REPLACE 实现覆盖写入
                df_to_write.to_sql(
                    "basic_info",
                    conn,
                    if_exists="append",
                    index=False,
                    method=SQLiteBaseStorage._upsert_method
                )
            
            logger.info(f"Basic info saved: {len(df_to_write)} records")
        except Exception as e:
            logger.error(f"Failed to write basic info: {e}")
            raise
    
    def _ensure_table_columns(self):
        """确保表包含所有需要的列，如果缺失则添加"""
        with self._get_connection() as conn:
            cursor = conn.execute("PRAGMA table_info(basic_info)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # 需要添加的列
            required_columns = {
                "list_status": "TEXT",
                "is_hs": "TEXT",
                "exchange": "TEXT"
            }
            
            for col_name, col_type in required_columns.items():
                if col_name not in columns:
                    try:
                        conn.execute(f"ALTER TABLE basic_info ADD COLUMN {col_name} {col_type}")
                        logger.debug(f"Added column {col_name} to basic_info table")
                    except Exception as e:
                        logger.debug(f"Could not add column {col_name}: {e}")
            
            conn.commit()

