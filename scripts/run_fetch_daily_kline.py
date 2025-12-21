import sys
import os
import dotenv
from datetime import datetime, timedelta
from loguru import logger

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

dotenv.load_dotenv()
from config import setup_logger
setup_logger()

from src.managers.daily_kline_manager import DailyKlineManager

def main():
    manager = DailyKlineManager()
    
    # 设置起始日期：最近一年
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    
    logger.info(f"Starting daily kline update from {start_date}...")
    manager.update_all(start_date=start_date)

if __name__ == "__main__":
    main()
