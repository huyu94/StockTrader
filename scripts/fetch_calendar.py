import sys
import os
import argparse
import dotenv
from datetime import datetime, timedelta
from loguru import logger

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

dotenv.load_dotenv()

from src.fetch.fetch_manager import FetchManager




if __name__ == "__main__":
    # main()
    # fetch_kline_data()
    fetch_manager = FetchManager()
    fetch_manager.update_calendar()