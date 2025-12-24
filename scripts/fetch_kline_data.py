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
from src.models import DateHelper

def main():
    """
    全量数据爬取任务调度脚本
    
    调用 Manager.update_all() 一键更新所有数据：
    1. 股票基本信息 (BasicInfo)
    2. 交易日历 (Calendar)
    3. 日线行情 (DailyKline)
    
    更新模式：
    - code模式：使用 pro_bar API 按股票代码获取过去一年的数据
      * 遍历所有股票，每只股票调用一次 pro_bar 获取全部历史数据
      * 适合首次全量爬取，数据完整
    - date模式：使用 pro.daily API 按交易日获取所有股票数据
      * 遍历所有交易日，每个交易日调用一次 pro.daily 获取全市场数据
      * 适合增量更新，补充特定日期的数据
    
    策略：
    - 使用SQLite数据库存储，批量写入性能优化
    - 多线程并发插入数据库，提升写入性能
    - 自动处理依赖关系
    """
    parser = argparse.ArgumentParser(description='股票数据爬取脚本')
    parser.add_argument('--mode', type=str, default='code', choices=['code', 'date'], 
                        help='爬取模式: code=按股票代码爬取(使用pro_bar,默认), date=按交易日爬取(使用pro.daily)')
    parser.add_argument('--start-date', type=str, default=None,
                        help='开始日期，支持格式: YYYYMMDD 或 YYYY-MM-DD，默认近一年')
    parser.add_argument('--end-date', type=str, default=None,
                        help='结束日期，支持格式: YYYYMMDD 或 YYYY-MM-DD，默认运行当天日期')
    args = parser.parse_args()
    
    try:
        # 在 scripts 层统一处理日期格式，转换为 YYYYMMDD（项目统一格式）
        start_date = None
        end_date = None
        
        if args.start_date:
            try:
                start_date = DateHelper.normalize(args.start_date)
                logger.info(f"Start date normalized: {args.start_date} -> {start_date}")
            except ValueError as e:
                logger.error(f"Invalid start_date format: {e}")
                return
        
        if args.end_date:
            try:
                end_date = DateHelper.normalize(args.end_date)
                logger.info(f"End date normalized: {args.end_date} -> {end_date}")
            except ValueError as e:
                logger.error(f"Invalid end_date format: {e}")
                return
        
        # 初始化统一的数据管理器（全部使用SQLite）
        data_manager = Manager()
        
        # 一键更新所有数据（传入的日期已经是 YYYYMMDD 格式）
        data_manager.update_all(start_date=start_date, end_date=end_date)
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Job interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}")
        raise

if __name__ == "__main__":
    main()
