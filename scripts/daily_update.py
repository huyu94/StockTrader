#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日自动更新股票数据脚本
"""

import sys
import os
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

# 配置日志
logger.add(
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'daily_update.log'),
    rotation='daily',
    level='INFO',
    encoding='utf-8'
)


def daily_update_job():
    """
    每日更新任务
    """
    try:
        logger.info("=== 开始执行每日自动更新任务 ===")
        
        # 创建数据获取器实例
        fetcher = StockDailyKLineFetcher()
        
        # 执行每日更新
        fetcher.daily_update()
        
        logger.info("=== 每日自动更新任务执行完成 ===")
    except Exception as e:
        logger.error(f"每日自动更新任务执行失败：{e}")
        raise


def main():
    """
    主函数
    """
    logger.info("=== 启动每日自动更新服务 ===")
    
    # 创建调度器
    scheduler = BlockingScheduler(timezone='Asia/Shanghai')
    
    # 添加定时任务
    # 每天16:00执行更新（股市收盘后）
    scheduler.add_job(
        daily_update_job,
        CronTrigger(hour=16, minute=0, second=0),
        id='daily_update_stock_data',
        name='每日更新股票数据',
        replace_existing=True
    )
    
    logger.info("每日自动更新服务已启动，将在每天16:00执行更新")
    logger.info(f"当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 启动调度器
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("每日自动更新服务已停止")
        scheduler.shutdown()
    except Exception as e:
        logger.error(f"每日自动更新服务运行出错：{e}")
        scheduler.shutdown()
        raise


if __name__ == "__main__":
    main()
