"""
每日定时调度脚本

每天晚上19:00执行 daily_pipeline，更新历史数据（股票基本信息、交易日历、日K线、复权因子、前复权数据）
注意：实时K线数据更新已移至 StrategyPipeline
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.orchestrator.scheduler import TaskScheduler
from core.pipelines.daily_pipeline import DailyPipeline
from utils.setup_logger import setup_logger


def run_daily_pipeline():
    """
    执行每日更新流水线
    
    更新历史数据：股票基本信息、交易日历、日K线、复权因子、前复权数据
    """
    try:
        logger.info("=" * 60)
        logger.info("开始执行每日更新流水线（历史数据）")
        logger.info("=" * 60)
        
        pipeline = DailyPipeline()
        pipeline.run(
            update_basic_info=True,
            update_trade_calendar=True,
            update_daily_kline=True,
            update_adj_factor=True,
            update_qfq_data=True
        )
        
        logger.info("=" * 60)
        logger.info("每日更新流水线执行完成")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"每日更新流水线执行失败: {e}")
        raise


def main():
    run_daily_pipeline()

    
if __name__ == "__main__":
    main()

