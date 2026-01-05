"""
调试策略筛选脚本

用于快速测试和调试策略筛选功能，只处理少量股票以便查看详细日志
"""

import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.pipelines.strategy_pipeline import StrategyPipeline
from core.strategies.kdj_strategy import KDJStrategy
from utils.setup_logger import setup_logger
from utils.date_helper import DateHelper
from core.loaders.basic_info import BasicInfoLoader


def debug_strategy():
    """
    调试策略筛选
    """
    # 设置日志为DEBUG级别，以便查看所有调试信息
    setup_logger(level_console="DEBUG", level_file="DEBUG")
    
    logger.info("=" * 80)
    logger.info("开始调试策略筛选")
    logger.info("=" * 80)
    
    # 创建策略实例
    strategy = KDJStrategy(
        kdj_period=9,
        vol_period=20,
        j_threshold=5.0,
        ma_tolerance=0.03
    )
    
    # 创建策略流水线
    pipeline = StrategyPipeline(
        config={
            'output_dir': 'output',
            'output_format': 'csv',
            'use_multiprocessing': False,  # 禁用多进程，便于查看调试信息
            'max_workers': 1
        }
    )
    
    # 获取今天的日期
    today = DateHelper.today()
    start_date = DateHelper.days_ago(365)
    logger.info(f"运行日期: {today}")
    logger.info(f"历史数据开始日期: {start_date}")
    
    # 获取少量股票进行测试（前10只）
    try:
        basic_info_loader = BasicInfoLoader()
        all_ts_codes = basic_info_loader.get_all_ts_codes()
        test_ts_codes = all_ts_codes[:10] if len(all_ts_codes) > 10 else all_ts_codes
        logger.info(f"测试股票数量: {len(test_ts_codes)}")
        logger.info(f"测试股票代码: {test_ts_codes}")
    except Exception as e:
        logger.warning(f"获取股票列表失败: {e}，将处理所有股票")
        test_ts_codes = None
    
    logger.info("=" * 80)
    logger.info("开始运行策略（不更新实时数据，仅测试历史数据）")
    logger.info("=" * 80)
    
    # 运行策略（不更新实时数据，仅测试）
    try:
        result = pipeline.run(
            strategy=strategy,
            ts_codes=test_ts_codes,  # 只处理测试股票
            trade_date=today,
            start_date=start_date,
            update_real_time_data=False  # 不更新实时数据，加快测试速度
        )
        
        # 打印结果
        logger.info("=" * 80)
        logger.info("策略筛选结果")
        logger.info("=" * 80)
        
        if isinstance(result, list):
            logger.info(f"筛选出 {len(result)} 只股票: {result}")
        elif hasattr(result, 'empty'):
            if not result.empty:
                logger.info(f"筛选出 {len(result)} 只股票")
                logger.info(f"\n{result.to_string()}")
            else:
                logger.info("未筛选出符合条件的股票")
        else:
            logger.info("未筛选出符合条件的股票")
        
    except Exception as e:
        logger.error(f"策略执行失败: {e}", exc_info=True)
        raise
    
    logger.info("=" * 80)
    logger.info("调试完成")
    logger.info("=" * 80)


if __name__ == "__main__":
    debug_strategy()

