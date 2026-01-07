"""
主入口文件

用于 Docker 部署，包含两个定时任务调度器：
1. 每天晚上19:00自动执行 daily_pipeline，更新历史数据
2. 每个交易日下午14:00自动执行 strategy_pipeline，运行策略筛选
"""

import signal
import sys
from loguru import logger
from core.pipelines.history_pipeline import HistoryPipeline
from core.pipelines.daily_pipeline import DailyPipeline
from core.pipelines.strategy_pipeline import StrategyPipeline
from core.strategies.kdj_strategy import KDJStrategy
from core.strategies.uptrend_pullback_strategy import UptrendPullbackStrategy
from core.orchestrator.scheduler import TaskScheduler
from core.loaders.trade_calendar import TradeCalendarLoader
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
    # 可以添加更多策略配置
    # {
    #     'strategy_class': AnotherStrategy,
    #     'strategy_params': {...},
    #     'start_date_days': 365,
    #     'name': '另一个策略'
    # },
]


def is_trading_day(trade_date: str) -> bool:
    """
    检查指定日期是否为交易日
    
    Args:
        trade_date: 交易日期 (YYYY-MM-DD)
        
    Returns:
        bool: 是否为交易日
    """
    try:
        # 创建交易日历加载器
        calendar_loader = TradeCalendarLoader()
        
        # 查询交易日历
        calendar_df = calendar_loader.read(cal_date=trade_date)
        
        if calendar_df is None or calendar_df.empty:
            logger.warning(f"无法查询交易日历，日期: {trade_date}，假设为交易日继续执行")
            return True  # 容错处理：如果查询失败，假设是交易日继续执行
        
        # 检查上交所或深交所是否开市
        # 只要有一个交易所开市，就认为是交易日
        is_trading = False
        if 'sse_open' in calendar_df.columns:
            sse_open = calendar_df['sse_open'].iloc[0]
            is_trading = is_trading or (sse_open == 1 or sse_open is True)
        if 'szse_open' in calendar_df.columns:
            szse_open = calendar_df['szse_open'].iloc[0]
            is_trading = is_trading or (szse_open == 1 or szse_open is True)
        
        return is_trading
        
    except Exception as e:
        logger.warning(f"检查交易日失败，日期: {trade_date}，错误: {e}，假设为交易日继续执行")
        return True  # 容错处理：如果检查失败，假设是交易日继续执行


def update_history():
    """
    执行历史数据补全
    """
    # 创建流水线实例
    history_pipeline = HistoryPipeline()

    # 执行历史数据补全
    history_pipeline.run(
        stock_codes=None,
        start_date="2015-01-01",
        end_date="2026-01-01",
        update_basic_info=False,      # 可选，默认 True
        update_trade_calendar=False,  # 可选，默认 True
        update_daily_kline=False,     # 可选，默认 True
        update_adj_factor=False,     # 可选，默认 True
        update_qfq_data=True         # 可选，默认 True
    )


def run_daily_pipeline():
    """
    执行每日更新流水线
    
    更新历史数据：股票基本信息、交易日历、日K线、复权因子、前复权数据
    注意：实时K线数据更新已移至 StrategyPipeline
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
            update_real_time_data=True  # 先更新实时K线数据
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
    
    使用 StrategyPipeline 的多策略并行运行功能
    只在交易日执行
    """
    try:
        # 检查是否为交易日
        today = DateHelper.today()
        if not is_trading_day(today):
            logger.info(f"今日 {today} 不是交易日，跳过策略执行")
            return
        
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
            ts_codes=None,  # 处理所有股票
            trade_date=None,  # 使用今天
            update_real_time_data=True,  # 统一更新实时K线数据
            send_to_robots=True
        )
        
        logger.info("=" * 80)
        logger.info("所有策略执行完成")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"策略流水线调度任务执行失败: {e}")
        raise


def main():
    """
    主函数：启动调度器
    
    用于 Docker 部署，启动定时任务调度器
    1. 每天晚上19:00自动执行每日数据更新
    2. 每个交易日下午14:00自动执行策略筛选
    """
    scheduler = None
    
    try:
        # 设置日志
        setup_logger()
        
        # 创建调度器
        scheduler = TaskScheduler(config={'timezone': 'Asia/Shanghai'})
        
        # 添加每日更新任务（每天19:00执行）
        scheduler.add_task({
            'name': 'daily_pipeline',
            'func': run_daily_pipeline,
            'schedule': '0 19 * * *',  # 每天19:00
            'args': (),
            'kwargs': {}
        })
        
        # 添加策略流水线任务（每天14:00执行，但内部会检查是否为交易日）
        scheduler.add_task({
            'name': 'strategy_pipeline',
            'func': run_all_strategies,
            'schedule': '0 14 * * *',  # 每天14:00
            'args': (),
            'kwargs': {}
        })
        
        logger.info("=" * 80)
        logger.info("定时任务调度器已启动")
        logger.info("=" * 80)
        logger.info("任务1: 每日数据更新")
        logger.info("  调度时间: 每天19:00")
        logger.info("  功能: 更新历史数据（股票基本信息、交易日历、日K线、复权因子、前复权数据）")
        logger.info("")
        logger.info("任务2: 策略流水线")
        logger.info("  调度时间: 每天14:00（仅交易日执行）")
        logger.info(f"  配置的策略数量: {len(STRATEGIES_CONFIG)}")
        for i, config in enumerate(STRATEGIES_CONFIG, 1):
            logger.info(f"    策略 {i}: {config.get('name', config['strategy_class'].__name__)}")
        logger.info("  功能: 更新实时K线数据，然后运行策略筛选")
        logger.info("=" * 80)
        logger.info("按 Ctrl+C 停止调度器")
        
        # 启动调度器
        scheduler.start()
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断信号，正在退出...")
        if scheduler:
            scheduler.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序运行异常: {e}")
        if scheduler:
            scheduler.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()