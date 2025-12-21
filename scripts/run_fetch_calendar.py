import sys
import os
import argparse
import dotenv
from loguru import logger
dotenv.load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import setup_logger
from src.managers.calendar_manager import CalendarManager
setup_logger()


def main():
    manager = CalendarManager()
    manager.calendar
    # logger.info(manager.sse_calendar)
    # logger.info(manager.szse_calendar)
    # logger.info()



if __name__ == "__main__":
    main()