import sys
import os
import argparse
import dotenv
from datetime import datetime, timedelta
from loguru import logger

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

dotenv.load_dotenv()
from config import setup_logger
setup_logger()

from src.manager import Manager

def main():
    """
    全量数据爬取任务调度脚本
    
    执行顺序：
    1. 股票基本信息 (BasicInfo)
    2. 交易日历 (Calendar)
    3. 日线行情 (DailyKline)
    4. 复权因子 (AdjFactor) - 暂时禁用
    
    策略：
    - 直接暴力爬取近一年的股票日k线前复权数据
    - 使用SQLite数据库存储，批量写入性能优化
    - 自动处理依赖关系
    """
    parser = argparse.ArgumentParser(description='股票数据爬取脚本')
    args = parser.parse_args()
    
    try:
        logger.info(f"🚀 Starting master data fetch job.")
        logger.info(f"📅 Time range: 近一年数据")
        logger.info(f"📊 Update mode: Full update by stock code (暴力爬取)")
        logger.info(f"💾 Storage mode: SQLite (fast batch writes)")
        
        # 初始化统一的数据管理器（全部使用SQLite）
        data_manager = Manager()
        
        # 1. Basic Info
        logger.info("Step 1/3: Fetching Basic Info...")
        data_manager.update_basic_info()
        stocks = data_manager.all_basic_info
        if stocks is not None and not stocks.empty:
            logger.success(f"✅ Basic Info updated. Total stocks: {len(stocks)}")
        else:
            logger.warning("⚠️ Basic Info updated but no stocks found in database")
        
        # 2. Calendar
        logger.info("Step 2/3: Fetching Trade Calendar...")
        data_manager.update_calendar()
        logger.success("✅ Trade Calendar updated.")
        
        # 3. Daily Kline - 直接调用update_daily_kline，总是执行全量更新
        logger.info("Step 3/3: Fetching Daily Kline Data...")
        data_manager.update_daily_kline()
        logger.success("✅ Daily Kline Data updated.")
        
        
        logger.success("🎉 All data fetch tasks completed successfully.")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Job interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}")
        raise

if __name__ == "__main__":
    main()
