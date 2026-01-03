"""
少妇战法策略Demo

运行少妇战法策略，筛选符合条件的股票
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from loguru import logger
from core.pipelines.strategy_pipeline import StrategyPipeline
from core.strategies.kdj_strategy import KDJStrategy
from utils.setup_logger import setup_logger
from utils.date_helper import DateHelper


def main():
    """
    主函数：运行少妇战法策略Demo
    """
    # 设置日志
    setup_logger()
    
    try:
        logger.info("=" * 60)
        logger.info("少妇战法策略Demo")
        logger.info("=" * 60)
        
        # 创建策略实例
        strategy = KDJStrategy(
            kdj_period=9,
            vol_period=20,
            j_threshold=5.0,
            ma_tolerance=0.03
        )
        
        # 创建策略流水线
        # 配置多进程：如果处理大量股票，可以启用多进程加速
        pipeline = StrategyPipeline(
            config={
                'output_dir': 'output',
                'output_format': 'csv',
                'use_multiprocessing': True,  # 启用多进程并行处理
                'max_workers': None  # None表示使用CPU核心数，也可以指定具体数字如4
            }
        )
        
        # 获取今天的日期
        today = DateHelper.today()
        one_years_ago = DateHelper.days_ago(365)
        logger.info(f"运行日期: {today}")
        
        # 运行策略
        # 注意：这里可以指定股票代码列表，如果不指定则处理所有股票
        # 为了demo快速运行，可以指定几只股票进行测试
        # ts_codes = ['000001.SZ', '000002.SZ', '600000.SH']  # 示例股票代码
        ts_codes = None  # 处理所有股票（可能需要较长时间）
        
        result = pipeline.run(
            strategy=strategy,
            ts_codes=ts_codes,
            trade_date=today,
            start_date=one_years_ago,  # 如果不指定，则从最早的数据开始
            # end_date=None,    # 如果不指定，则使用trade_date的前一天
        )
        
        # 打印结果
        logger.info("=" * 60)
        logger.info("策略筛选结果")
        logger.info("=" * 60)
        
        if isinstance(result, list):
            logger.info(f"筛选出 {len(result)} 只股票:")
            for ts_code in result:
                logger.info(f"  - {ts_code}")
        elif isinstance(result, pd.DataFrame) and not result.empty:
            logger.info(f"筛选出 {len(result)} 只股票:")
            logger.info(f"\n{result.to_string()}")
        else:
            logger.info("未筛选出符合条件的股票")
        
        logger.info("=" * 60)
        logger.info("Demo执行完成！")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Demo执行失败: {e}")
        raise


if __name__ == "__main__":
    main()

