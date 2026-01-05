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
    # 可以添加更多策略配置
    # {
    #     'strategy_class': AnotherStrategy,
    #     'strategy_params': {...},
    #     'start_date_days': 365,
    #     'name': '另一个策略'
    # },
]


def run_strategy_pipeline(strategy_config: dict):
    """
    执行单个策略的流水线
    
    先更新实时K线数据，然后运行策略筛选
    
    Args:
        strategy_config: 策略配置字典，包含：
            - strategy_class: 策略类
            - strategy_params: 策略初始化参数
            - start_date_days: 历史数据开始日期（天数）
            - name: 策略名称（可选）
    """
    try:
        strategy_class = strategy_config['strategy_class']
        strategy_params = strategy_config.get('strategy_params', {})
        start_date_days = strategy_config.get('start_date_days', 365)
        strategy_name = strategy_config.get('name', strategy_class.__name__)
        
        logger.info("=" * 60)
        logger.info(f"开始执行策略: {strategy_name}")
        logger.info("=" * 60)
        
        # 创建策略实例
        strategy = strategy_class(**strategy_params)
        
        # 创建策略流水线
        pipeline = StrategyPipeline(
            config={
                'output_dir': 'output',
                'output_format': 'csv',
                'use_multiprocessing': True,
                'max_workers': None  # 使用CPU核心数
            }
        )
        
        # 获取今天的日期
        today = DateHelper.today()
        start_date = DateHelper.days_ago(start_date_days)
        logger.info(f"运行日期: {today}")
        logger.info(f"历史数据开始日期: {start_date}")
        
        # 运行策略（会自动先更新实时K线数据，因为 update_real_time_data=True）
        result = pipeline.run(
            strategy=strategy,
            ts_codes=None,  # 处理所有股票
            trade_date=today,
            start_date=start_date,
            update_real_time_data=False  # 先更新实时K线数据
        )
        
        # 打印结果摘要
        logger.info("=" * 60)
        logger.info(f"策略 {strategy_name} 筛选结果")
        logger.info("=" * 60)
        
        if isinstance(result, list):
            logger.info(f"筛选出 {len(result)} 只股票")
        elif hasattr(result, 'empty') and not result.empty:
            logger.info(f"筛选出 {len(result)} 只股票")
        else:
            logger.info("未筛选出符合条件的股票")
        
        logger.info("=" * 60)
        logger.info(f"策略 {strategy_name} 执行完成")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"策略执行失败: {strategy_config.get('name', 'Unknown')}, 错误: {e}")
        raise


def run_all_strategies():
    """
    执行所有配置的策略
    
    依次运行每个策略，每个策略都会先更新实时K线数据
    """
    try:
        logger.info("=" * 80)
        logger.info("开始执行策略流水线调度任务")
        logger.info(f"共配置 {len(STRATEGIES_CONFIG)} 个策略")
        logger.info("=" * 80)
        
        for i, strategy_config in enumerate(STRATEGIES_CONFIG, 1):
            logger.info(f"\n执行第 {i}/{len(STRATEGIES_CONFIG)} 个策略")
            try:
                run_strategy_pipeline(strategy_config)
            except Exception as e:
                logger.error(f"策略 {strategy_config.get('name', 'Unknown')} 执行失败: {e}")
                # 继续执行下一个策略，不中断整个流程
                continue
        
        logger.info("=" * 80)
        logger.info("所有策略执行完成")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"策略流水线调度任务执行失败: {e}")
        raise


def main():

    setup_logger()
    run_all_strategies()


if __name__ == "__main__":
    main()

