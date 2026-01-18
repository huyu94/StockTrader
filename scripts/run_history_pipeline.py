"""
历史数据补全脚本

用于补全历史数据，包括：
- 股票基本信息 (basic_info)
- 交易日历 (trade_calendar)
- 日K线数据 (daily_kline)
- 复权因子 (adj_factor)
- 前复权数据 (qfq_data)
"""

import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.pipelines.history_pipeline import HistoryPipeline
from utils.setup_logger import setup_logger


def run_history_pipeline(
    start_date: str = "2015-01-01",
    end_date: str = "2026-01-01",
    update_basic_info: bool = False,
    update_trade_calendar: bool = False,
    update_daily_kline: bool = False,
    update_adj_factor: bool = False,
    update_qfq_data: bool = True
):
    """
    执行历史数据补全流水线
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)，默认 "2015-01-01"
        end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)，默认 "2026-01-01"
        update_basic_info: 是否更新股票基本信息，默认 False
        update_trade_calendar: 是否更新交易日历，默认 False
        update_daily_kline: 是否更新日K线数据，默认 False
        update_adj_factor: 是否更新复权因子，默认 False
        update_qfq_data: 是否更新前复权数据，默认 True
    """
    try:
        logger.info("=" * 60)
        logger.info("开始执行历史数据补全流水线")
        logger.info("=" * 60)
        
        # 创建流水线实例
        history_pipeline = HistoryPipeline()
        
        # 执行历史数据补全
        history_pipeline.run(
            start_date=start_date,
            end_date=end_date,
            update_basic_info=update_basic_info,
            update_trade_calendar=update_trade_calendar,
            update_daily_kline=update_daily_kline,
            update_adj_factor=update_adj_factor,
            update_qfq_data=update_qfq_data
        )
        
        logger.info("=" * 60)
        logger.info("历史数据补全流水线执行完成")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"历史数据补全流水线执行失败: {e}")
        raise


def main():
    """
    主函数
    
    可以通过修改下面的参数来配置要更新的数据类型
    """
    # 设置日志
    setup_logger()
    
    # 执行历史数据补全
    # 默认只更新前复权数据，如果需要更新其他数据，请修改下面的参数
    run_history_pipeline(
        start_date="2026-01-01",
        end_date="2026-01-18",
        update_basic_info=True,      # 是否更新股票基本信息
        update_trade_calendar=True,  # 是否更新交易日历
        update_daily_kline=True,      # 是否更新日K线数据
        update_adj_factor=True,       # 是否更新复权因子
        update_qfq_data=True          # 是否更新前复权数据
    )


if __name__ == "__main__":
    main()
