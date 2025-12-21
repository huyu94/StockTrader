import sys
import os
import argparse
import dotenv
from loguru import logger
dotenv.load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import setup_logger
setup_logger()
from src.managers.adj_factor_manager import AdjFactorManager
from src.managers.basic_info_manager import BasicInfoManager
# from src.managers.calendar_manager import CalendarManager


def main():
    manager = AdjFactorManager()
    # logger.info(manager.sse_calendar)
    # logger.info(manager.szse_calendar)
    basic_info_manager = BasicInfoManager()
    ts_codes = basic_info_manager.get_stocks_code_by_market(["主板", "创业板", "科创板", "CDR", "北交所"])
    logger.info(len(ts_codes))
    manager.batch_get_adj_factors(ts_codes=ts_codes)
    # manager.get_adj_factor(ts_code="000001.SZ")

if __name__ == "__main__":
    main()
