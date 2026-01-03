"""
策略工作进程辅助模块

用于多进程并行处理股票策略计算
"""

from typing import Union, List, Dict, Any
import pandas as pd
from loguru import logger

from core.strategies.base import BaseStrategy


def process_single_stock(
    ts_code: str,
    stock_df: pd.DataFrame,
    strategy_class: type,
    strategy_params: Dict[str, Any]
) -> Union[List[str], pd.DataFrame, None]:
    """
    处理单只股票的策略计算（用于多进程）
    
    这个函数需要能够被pickle序列化，因此不能直接传递策略实例，
    而是传递策略类和参数，在子进程中重新创建策略实例。
    
    Args:
        ts_code: 股票代码
        stock_df: 该股票的K线数据DataFrame
        strategy_class: 策略类（必须是BaseStrategy的子类）
        strategy_params: 策略初始化参数字典
    
    Returns:
        Union[List[str], pd.DataFrame, None]: 
            - 如果股票符合条件，返回筛选结果（List或DataFrame）
            - 如果不符合条件，返回None
    """
    try:
        # 在子进程中创建策略实例
        strategy = strategy_class(**strategy_params)
        
        # 确保数据按日期排序
        if 'trade_date' in stock_df.columns:
            stock_df = stock_df.sort_values('trade_date').reset_index(drop=True)
        
        # 运行策略：计算指标并筛选
        result = strategy.run(stock_df)
        
        # 如果结果是列表
        if isinstance(result, list):
            # 如果当前股票代码在结果列表中，返回包含该股票代码的列表
            if ts_code in result:
                return [ts_code]
            else:
                return None
        
        # 如果结果是DataFrame
        elif isinstance(result, pd.DataFrame):
            # 如果DataFrame不为空，说明这只股票符合条件（因为传入的是单只股票的数据）
            if not result.empty:
                # 确保包含ts_code列
                if 'ts_code' not in result.columns:
                    # 如果没有ts_code列，添加它
                    result = result.copy()
                    result['ts_code'] = ts_code
                return result
            else:
                return None
        
        return None
        
    except Exception as e:
        logger.error(f"处理股票 {ts_code} 时出错: {e}")
        return None

