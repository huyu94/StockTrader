"""
初始化SQLite数据库
创建所有必要的表结构
"""
import sys
import os
from loguru import logger

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import setup_logger
setup_logger()

from src.data.storage.daily_kline_storage_sqlite import DailyKlineStorageSQLite
from src.data.storage.basic_info_storage_sqlite import BasicInfoStorageSQLite
from src.data.storage.calendar_storage_sqlite import CalendarStorageSQLite


def init_database():
    """初始化所有数据库表"""
    logger.info("Initializing SQLite database...")
    
    try:
        # 初始化所有存储类（会自动创建表结构）
        logger.info("Creating daily_kline table...")
        daily_storage = DailyKlineStorageSQLite()
        logger.success("✅ daily_kline table created")
        
        logger.info("Creating adj_factor table...")
        adj_storage = AdjFactorStorageSQLite()
        logger.success("✅ adj_factor table created")
        
        logger.info("Creating basic_info table...")
        basic_storage = BasicInfoStorageSQLite()
        logger.success("✅ basic_info table created")
        
        logger.info("Creating trade_calendar table...")
        calendar_storage = CalendarStorageSQLite()
        logger.success("✅ trade_calendar table created")
        
        logger.success("🎉 All database tables initialized successfully!")
        logger.info(f"Database location: {daily_storage.db_path}")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


if __name__ == "__main__":
    init_database()

