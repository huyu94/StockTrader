"""
主程序：使用 StrategyRunner 运行策略

支持两种模式：
1. 单股票运行：检查指定股票在指定日期是否符合策略
2. 批量运行：批量检查所有股票，筛选符合条件的股票池
"""

from src.runner import StrategyRunner
from src.strategies.kdj_strategy import KDJStrategy
from loguru import logger
from config import setup_logger

setup_logger()


def run_single():
    """单股票运行示例"""
    runner = StrategyRunner()
    
    result = runner.run_single(
        strategy_class=KDJStrategy,
        ts_code="300139.SZ",
        target_date="20251219",
        start_date="20250201",
        end_date="20251222",
        strategy_kwargs={
            "kdj_period": 9,
            "vol_period": 20,
            "j_threshold": 5.0
        }
    )
    
    # 显示结果
    print("\n" + "="*80)
    print("【策略执行结果】")
    print("="*80)
    print(f"股票代码: {result['ts_code']}")
    print(f"股票名称: {result.get('stock_name', '未知')}")
    print(f"检查日期: {result['target_date']}")
    print(f"交易信号: {result['signal']}")
    print(f"是否成功: {result['success']}")
    
    if result["success"]:
        explanation = result.get("explanation", {})
        if "reason" in explanation:
            print(f"策略解释: {explanation['reason']}")
    else:
        print(f"错误信息: {result.get('error', '未知错误')}")
    
    print("="*80)


def run_batch():
    """批量运行示例"""
    runner = StrategyRunner()
    
    stock_pool = runner.run_batch(
        strategy_class=KDJStrategy,
        target_date="20251224",
        start_date="20250201",
        end_date="20251224",
        strategy_kwargs={
            "kdj_period": 9,
            "vol_period": 20,
            "j_threshold": 5.0
        },
        use_concurrent=True,  # 默认关闭并发，如需启用请测试
        max_workers=4,  # 并发时的线程数（仅在use_concurrent=True时有效）
        save_results=True,
        output_filename="stock_pool"
    )
    
    print(f"\n找到 {len(stock_pool)} 只符合条件的股票")
    return stock_pool


if __name__ == "__main__":
    # 选择运行模式
    mode = "batch"  # 可选: "single" 或 "batch"
    
    if mode == "single":
        run_single()
    elif mode == "batch":
        stock_pool = run_batch()
    else:
        logger.error(f"未知的运行模式: {mode}")
