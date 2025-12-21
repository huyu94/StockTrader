
import sys
import os
import argparse
import dotenv
from loguru import logger
dotenv.load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import setup_logger
setup_logger()
from src.managers.basic_info_manager import BasicInfoManager
# from src.managers.calendar_manager import CalendarManager


def main():
    manager = BasicInfoManager()
    # logger.info(manager.sse_calendar)
    # logger.info(manager.szse_calendar)
    ts_codes = ["000001.SZ", "000002.SZ"]
    # manager.batch_get_basic_info(ts_codes=ts_codes)
    basic_info = manager.all_basic_info
    stock_codes = manager.all_stock_codes
    logger.info(basic_info)
    logger.info(len(stock_codes))

if __name__ == "__main__":
    main()