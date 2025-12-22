from numpy import True_
import pandas as pd
from typing import Optional, List, Union
from loguru import logger
from .sqlite_base import SQLiteBaseStorage
from .sql_models import DAILY_KLINE_TABLE, DAILY_KLINE_INDEXES
from src.models.stock_models import DailyKlineData
import dotenv

dotenv.load_dotenv()


class DailyKlineStorageSQLite(SQLiteBaseStorage):
    """日线行情存储管理器（SQLite版本）
    使用SQLite数据库替代CSV文件，大幅提升批量写入性能
    
    优势：
    1. 单文件存储，减少文件打开/关闭开销
    2. 支持事务批量写入，性能提升10-100倍
    3. 支持索引，查询更快
    4. 支持UPSERT（INSERT OR REPLACE），自动去重
    """
    
    def __init__(self, db_name: str = "stock_data.db"):
        super().__init__(db_name)
    
    def _init_database(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            # 使用 SQL 模型定义创建表
            conn.execute(DAILY_KLINE_TABLE)
            
            # 如果表已存在但没有adj_factor列，则添加该列
            try:
                cursor = conn.execute("PRAGMA table_info(daily_kline)")
                columns = [row[1] for row in cursor.fetchall()]
                if "adj_factor" not in columns:
                    conn.execute("ALTER TABLE daily_kline ADD COLUMN adj_factor REAL")
                    conn.commit()
                    logger.debug("Added adj_factor column to existing daily_kline table")
            except Exception as e:
                # 忽略错误（列可能已存在或其他原因）
                logger.debug(f"Could not add adj_factor column (may already exist): {e}")
            
            # 创建索引
            for index_sql in DAILY_KLINE_INDEXES:
                conn.execute(index_sql)
            
            conn.commit()
            logger.debug(f"Database initialized: {self.db_path}")
    
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
            
            # 统一日期格式为 YYYYMMDD（与数据库存储格式一致）
            from src.utils.date_helper import DateHelper
            try:
                start_date_normalized = DateHelper.normalize(start_date)
                end_date_normalized = DateHelper.normalize(end_date)
            except Exception as e:
                logger.error(f"Invalid date format for {ts_code}: start={start_date}, end={end_date}, error={e}")
                return pd.DataFrame()
            
            with self._get_connection() as conn:
                query = "SELECT * FROM daily_kline WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ? ORDER BY trade_date"
                df = pd.read_sql_query(query, conn, params=(ts_code, start_date_normalized, end_date_normalized))
                
                if df.empty:
                    return pd.DataFrame()
                
                # 转换trade_date为datetime（支持 YYYY-MM-DD 和 YYYYMMDD 格式）
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                return df
        except Exception as e:
            logger.error(f"Failed to load daily kline for {ts_code}: {e}")
            return pd.DataFrame()
    
    def write(self, df: pd.DataFrame) -> bool:
        """
        写入股票数据（统一接口，带重试机制）
        
        注意：
        - 假设传入的 DataFrame 已经是正确格式，不做额外验证
        - 数据验证应该在外层使用 Pydantic 模型完成
        - DataFrame 必须包含 ts_code 列
        - 支持单只股票或多只股票的数据写入
        - 遇到 database locked 错误会自动重试
        
        :param df: 股票数据 DataFrame，必须包含 ts_code 和 trade_date 列
        :return: True 表示成功，False 表示失败
        """
        import time
        
        if df.empty:
            return True
        
        # 重试配置
        max_retries = 5
        retry_delay = 2  # 秒
        
        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    # 使用 INSERT OR REPLACE 实现覆盖写入（自动去重）
                    df.to_sql(
                        "daily_kline",
                        conn,
                        if_exists="append",
                        index=False,
                        method=SQLiteBaseStorage._upsert_method
                    )
                
                # 写入成功
                if attempt > 0:
                    logger.info(f"✓ Write succeeded after {attempt + 1} attempts ({len(df)} rows)")
                return True
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # 判断是否是数据库锁错误
                if "locked" in error_msg or "database is locked" in error_msg:
                    if attempt < max_retries - 1:
                        # 还有重试机会，等待后重试
                        logger.warning(
                            f"Database locked (attempt {attempt + 1}/{max_retries}), "
                            f"retrying in {retry_delay}s... [{len(df)} rows pending]"
                        )
                        time.sleep(retry_delay)
                        continue
                    else:
                        # 已经重试了最大次数，放弃
                        logger.error(
                            f"❌ Failed to write after {max_retries} attempts due to database lock. "
                            f"Lost {len(df)} rows of data."
                        )
                        return False
                else:
                    # 其他类型的错误，不重试
                    logger.error(f"❌ Failed to write data (non-lock error): {e}")
                    return False
        
        return False
    

    def load_multiple(self, ts_codes: List[str]) -> pd.DataFrame:
        """批量读取多只股票的数据"""
        try:
            if not ts_codes:
                return pd.DataFrame()
            
            placeholders = ','.join(['?' for _ in ts_codes])
            with self._get_connection() as conn:
                query = f"""
                    SELECT * FROM daily_kline 
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, trade_date
                """
                df = pd.read_sql_query(query, conn, params=ts_codes)
                
                if df.empty:
                    return df
                
                # 转换trade_date为datetime（支持 YYYY-MM-DD 和 YYYYMMDD 格式）
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                return df
        except Exception as e:
            logger.error(f"Failed to load multiple stocks: {e}")
            return pd.DataFrame()
    
    def get_stock_count(self) -> int:
        """获取数据库中股票数量"""
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_kline")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get stock count: {e}")
            return 0
    
    def get_total_rows(self) -> int:
        """获取数据库总行数"""
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM daily_kline")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get total rows: {e}")
            return 0

