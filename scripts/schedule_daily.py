"""
每日定时调度脚本

每天下午2:00执行 daily_pipeline
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
    """
    try:
        logger.info("=" * 60)
        logger.info("开始执行每日更新流水线")
        logger.info("=" * 60)
        
        pipeline = DailyPipeline()
        pipeline.run(
            update_basic_info=True,
            update_trade_calendar=True,
            update_daily_kline=True,
            update_adj_factor=True,
            update_qfq_data=True,
            update_real_time_data=True
        )
        
        logger.info("=" * 60)
        logger.info("每日更新流水线执行完成")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"每日更新流水线执行失败: {e}")
        raise


def main():
    """
    主函数：启动调度器
    """
    # 设置日志
    setup_logger()
    
    # 创建调度器
    scheduler = TaskScheduler(config={'timezone': 'Asia/Shanghai'})
    
    # 添加每日更新任务（每天14:00执行）
    scheduler.add_task({
        'name': 'daily_pipeline',
        'func': run_daily_pipeline,
        'schedule': '0 14 * * *',  # 每天14:00
        'args': (),
        'kwargs': {}
    })
    
    # 启动调度器
    scheduler.start()


if __name__ == "__main__":
    main()

