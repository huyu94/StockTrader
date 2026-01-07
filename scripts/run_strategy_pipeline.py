"""
策略流水线定时调度脚本

每天下午14:00执行策略流水线，先更新实时K线数据，然后运行策略筛选
支持配置多个策略依次执行
"""

import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.orchestrator.scheduler import TaskScheduler
from core.pipelines.strategy_pipeline import StrategyPipeline
from core.strategies.kdj_strategy import KDJStrategy
from core.strategies.lianyang_strategy import LianyangStrategy
from core.strategies.uptrend_pullback_strategy import UptrendPullbackStrategy
from utils.setup_logger import setup_logger
from utils.date_helper import DateHelper


# 配置要运行的策略列表
# 每个策略配置包含：策略类、策略参数、历史数据开始日期（天数）
STRATEGIES_CONFIG = [
    {
        'strategy_class': KDJStrategy,
        'strategy_params': {
            'kdj_period': 9,
            'vol_period': 20,
            'j_threshold': 5.0,
            'ma_tolerance': 0.03
        },
        'start_date_days': 365,  # 使用最近365天的数据
        'name': '少妇战法'
    },
    {
        'strategy_class': UptrendPullbackStrategy,
        'strategy_params': {
            'kdj_period': 9,
            'j_threshold': 5.0,
            'vol_period': 20,
            'vol_shrink_ratio': 0.5,
            'ma_tolerance': 0.03
        },
        'start_date_days': 365,  # 使用最近365天的数据
        'name': '上升趋势回调买入'
    },
    # {
    #     'strategy_class': LianyangStrategy,
    #     'strategy_params': {
    #         'min_consecutive_days': 5,
    #         'max_price': 25.0,
    #         'max_market_cap': 300.0,
    #         'max_pe': 166.0,
    #         'min_turnover': 5.0,
    #         'max_5day_pct': 10.0,
    #         'min_limit_up_count': 2
    #     },
    #     'start_date_days': 365,  # 使用最近365天的数据
    #     'name': '连阳战法'
    # },
]


def run_all_strategies():
    """
    执行所有配置的策略
    
    使用 StrategyPipeline 的多策略并行运行功能
    """
    try:
        logger.info(f"开始执行策略流水线，共 {len(STRATEGIES_CONFIG)} 个策略")
        
        # 创建 StrategyPipeline 实例
        pipeline = StrategyPipeline(
            config={
                'output_dir': 'output',
                'output_format': 'csv',
                'use_multiprocessing': True,
                'max_workers': None  # 使用CPU核心数
            }
        )
        
        # 并行运行所有策略（内部会统一更新数据一次）
        results = pipeline.run(
            strategies_config=STRATEGIES_CONFIG,
            ts_codes=['300997.SZ'],  # 处理所有股票
            trade_date="20260105",  # 使用今天
            update_real_time_data=False,  # 统一更新实时K线数据
            send_to_robots=False
        )
        
        # 打印结果摘要
        # logger.info("=" * 60)
        # logger.info("所有策略执行完成，结果摘要")
        # logger.info("=" * 60)
        # # for strategy_name, result in results.items():
        # #     if isinstance(result, list):
        # #         logger.info(f"策略 {strategy_name} 筛选出 {len(result)} 只股票")
        # #     elif hasattr(result, 'empty') and not result.empty:
        # #         logger.info(f"策略 {strategy_name} 筛选出 {len(result)} 只股票")
        # #     else:
        # #         logger.info(f"策略 {strategy_name} 未筛选出符合条件的股票")
        # logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"策略流水线调度任务执行失败: {e}")
        raise


def main():

    setup_logger()
    run_all_strategies()


if __name__ == "__main__":
    main()

