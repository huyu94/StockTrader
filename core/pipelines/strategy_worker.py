"""
策略工作进程辅助模块

用于多进程并行处理股票策略计算
"""

from typing import Union, List, Dict, Any, Optional
import pandas as pd
from loguru import logger

from core.strategies.base import BaseStrategy
from core.loaders.daily_kline import DailyKlineLoader
from core.loaders.intraday_kline import IntradayKlineLoader
from core.calculators.aggregator import Aggregator


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


def process_single_stock_complete(
    ts_code: str,
    strategy_class: type,
    strategy_params: Dict[str, Any],
    start_date: Optional[str],
    end_date: Optional[str],
    trade_date: str,
    config: Optional[Dict[str, Any]] = None
) -> Union[List[str], pd.DataFrame, None]:
    """
    处理单只股票的完整流程（用于多进程）
    
    在子进程中完成从数据读取到策略筛选的完整流程：
    1. 读取历史K线数据
    2. 读取实时K线数据
    3. 聚合实时K线为日K线
    4. 拼接历史数据和当天数据
    5. 计算指标并筛选股票
    
    Args:
        ts_code: 股票代码
        strategy_class: 策略类（必须是BaseStrategy的子类）
        strategy_params: 策略初始化参数字典
        start_date: 历史数据开始日期 (YYYY-MM-DD)
        end_date: 历史数据结束日期 (YYYY-MM-DD)
        trade_date: 交易日期 (YYYY-MM-DD)
        config: 流水线配置字典（用于初始化数据加载器）
    
    Returns:
        Union[List[str], pd.DataFrame, None]: 
            - 如果股票符合条件，返回筛选结果（List或DataFrame）
            - 如果不符合条件，返回None
    """
    try:
        # 1. 初始化数据加载器和聚合器
        daily_kline_loader = DailyKlineLoader()
        intraday_kline_loader = IntradayKlineLoader()
        aggregator = Aggregator()
        
        # 2. 读取历史K线数据（包含今天的数据）
        historical_df = daily_kline_loader.read(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if historical_df.empty:
            logger.debug(f"[DEBUG] 股票 {ts_code}: 历史K线数据为空 (start_date={start_date}, end_date={end_date})")
            return None
        
        logger.debug(f"[DEBUG] 股票 {ts_code}: 读取到 {len(historical_df)} 条历史K线数据")
        
        # 确保 trade_date 列是 datetime 类型
        if 'trade_date' in historical_df.columns:
            if not pd.api.types.is_datetime64_any_dtype(historical_df['trade_date']):
                historical_df['trade_date'] = pd.to_datetime(historical_df['trade_date'], errors='coerce')
        
        # 3. 检查历史数据中是否包含今天的数据
        trade_date_obj = pd.to_datetime(trade_date)
        today_data_in_historical = historical_df[
            historical_df['trade_date'] == trade_date_obj
        ]
        
        if not today_data_in_historical.empty:
            logger.debug(f"[DEBUG] 股票 {ts_code}: 历史数据中包含今天({trade_date})的数据，共 {len(today_data_in_historical)} 条")
        else:
            logger.debug(f"[DEBUG] 股票 {ts_code}: 历史数据中不包含今天({trade_date})的数据")
        
        # 4. 准备最终数据
        if not today_data_in_historical.empty:
            # 历史数据中有今天的数据，优先使用历史数据
            # 过滤掉历史数据中今天的数据，然后添加回来（确保使用历史数据）
            historical_without_today = historical_df[
                historical_df['trade_date'] < trade_date_obj
            ]
            
            # 准备今天的历史数据
            today_historical_prepared = _prepare_final_dataframe(today_data_in_historical)
            
            # 准备历史数据（不包含今天）
            historical_prepared = _prepare_final_dataframe(historical_without_today)
            
            # 合并：历史数据（不含今天）+ 今天的历史数据
            if not historical_prepared.empty:
                final_df = pd.concat([historical_prepared, today_historical_prepared], ignore_index=True)
            else:
                final_df = today_historical_prepared
            
            logger.debug(f"[DEBUG] 股票 {ts_code}: 使用历史K线数据（优先），最终数据共 {len(final_df)} 条，日期范围: {final_df['trade_date'].min()} 到 {final_df['trade_date'].max()}")
        else:
            # 历史数据中没有今天的数据，使用实时K线数据
            # 5. 读取实时K线数据
            intraday_df = intraday_kline_loader.read(
                ts_code=ts_code,
                trade_date=trade_date
            )
            
            if intraday_df.empty:
                # 实时数据也没有，报错
                error_msg = f"股票 {ts_code} 在 {trade_date} 既没有历史K线数据，也没有实时K线数据"
                logger.error(f"[DEBUG] {error_msg}")
                raise ValueError(error_msg)
            
            logger.debug(f"[DEBUG] 股票 {ts_code}: 读取到 {len(intraday_df)} 条实时K线数据")
            
            # 6. 聚合实时k线数据为日K线
            daily_from_intraday = aggregator.aggregate_to_daily(intraday_df)
            
            if daily_from_intraday.empty:
                # 聚合失败，报错
                error_msg = f"股票 {ts_code} 在 {trade_date} 实时K线数据聚合失败"
                logger.error(f"[DEBUG] {error_msg}")
                raise ValueError(error_msg)
            
            logger.debug(f"[DEBUG] 股票 {ts_code}: 实时K线聚合成功，得到 {len(daily_from_intraday)} 条日K线数据")
            
            # 7. 合并历史数据和实时数据
            # 过滤掉历史数据中今天的数据（虽然应该没有，但为了安全）
            historical_without_today = historical_df[
                historical_df['trade_date'] < trade_date_obj
            ]
            
            logger.debug(f"[DEBUG] 股票 {ts_code}: 历史数据（不含今天）共 {len(historical_without_today)} 条")
            
            final_df = _merge_historical_and_realtime_single_stock(
                historical_df=historical_without_today,
                daily_from_intraday=daily_from_intraday,
                trade_date=trade_date
            )
            
            logger.debug(f"[DEBUG] 股票 {ts_code}: 使用实时K线聚合数据，最终数据共 {len(final_df)} 条，日期范围: {final_df['trade_date'].min()} 到 {final_df['trade_date'].max()}")
        
        if final_df.empty:
            logger.debug(f"[DEBUG] 股票 {ts_code}: 最终数据为空，跳过策略筛选")
            return None
        
        logger.debug(f"[DEBUG] 股票 {ts_code}: 准备运行策略，最终数据共 {len(final_df)} 条")
        
        # 5. 创建策略实例并运行
        strategy = strategy_class(**strategy_params)
        
        # 确保数据按日期排序
        if 'trade_date' in final_df.columns:
            final_df = final_df.sort_values('trade_date').reset_index(drop=True)
        
        # 运行策略：计算指标并筛选
        result = strategy.run(final_df)
        
        if result is None:
            logger.debug(f"[DEBUG] 股票 {ts_code}: 策略筛选结果为空")
        elif isinstance(result, list):
            logger.debug(f"[DEBUG] 股票 {ts_code}: 策略筛选结果包含 {len(result)} 个股票代码")
        elif isinstance(result, pd.DataFrame):
            logger.debug(f"[DEBUG] 股票 {ts_code}: 策略筛选结果DataFrame包含 {len(result)} 行")
        
        # 6. 处理结果
        if isinstance(result, list):
            # 如果结果是列表，检查当前股票是否在结果中
            if ts_code in result:
                return [ts_code]
            else:
                return None
        elif isinstance(result, pd.DataFrame):
            # 如果结果是DataFrame，检查是否为空
            if not result.empty:
                # 确保包含ts_code列
                if 'ts_code' not in result.columns:
                    result = result.copy()
                    result['ts_code'] = ts_code
                return result
            else:
                return None
        
        return None
        
    except Exception as e:
        logger.error(f"处理股票 {ts_code} 的完整流程时出错: {e}")
        return None


def _prepare_final_dataframe(historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    准备最终DataFrame（仅使用历史数据时）
    
    将历史数据的前复权价格字段转换为标准列名，并删除_qfq后缀列
    
    Args:
        historical_df: 历史日K线数据
    
    Returns:
        pd.DataFrame: 准备好的DataFrame（已删除_qfq后缀列）
    """
    df = historical_df.copy()
    
    # 如果存在前复权价格字段，使用它们替换标准价格字段
    if 'close_qfq' in df.columns:
        df['close'] = df['close_qfq']
    if 'open_qfq' in df.columns:
        df['open'] = df['open_qfq']
    if 'high_qfq' in df.columns:
        df['high'] = df['high_qfq']
    if 'low_qfq' in df.columns:
        df['low'] = df['low_qfq']
    
    # 删除_qfq后缀的列（聚合器mock操作后，应该只保留标准列名）
    qfq_columns = [col for col in df.columns if col.endswith('_qfq')]
    if qfq_columns:
        df = df.drop(columns=qfq_columns)
        logger.debug(f"已删除_qfq后缀列: {qfq_columns}")
    
    # 确保必需的列存在
    required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"数据缺少必需的列: {missing_columns}")
    
    return df


def _merge_historical_and_realtime_single_stock(
    historical_df: pd.DataFrame,
    daily_from_intraday: pd.DataFrame,
    trade_date: str
) -> pd.DataFrame:
    """
    拼接单只股票的历史k线和当天实时k线
    
    注意：此函数仅在历史数据中没有今天的数据时被调用。
    如果历史数据中有今天的数据，应该优先使用历史数据，不会调用此函数。
    
    Args:
        historical_df: 单只股票的历史日K线数据（不包含今天的数据，已过滤）
        daily_from_intraday: 单只股票的当天实时k线聚合后的日K线数据
        trade_date: 交易日期
    
    Returns:
        pd.DataFrame: 拼接后的DataFrame
    """
    # 如果历史数据为空，直接返回当天数据
    if historical_df.empty:
        return daily_from_intraday.copy()
    
    # 如果当天数据为空，直接返回历史数据
    if daily_from_intraday.empty:
        return _prepare_final_dataframe(historical_df)
    
    # 准备历史数据：使用前复权价格字段
    historical_prepared = _prepare_final_dataframe(historical_df)
    
    # 准备当天数据：直接使用聚合后的数据
    daily_prepared = daily_from_intraday.copy()
    
    # 确保当天数据包含所有必需的列
    required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
    for col in required_columns:
        if col not in daily_prepared.columns:
            if col == 'vol' and 'volume' in daily_prepared.columns:
                daily_prepared['vol'] = daily_prepared['volume']
            elif col == 'amount' and 'amount' in daily_prepared.columns:
                pass  # amount已经存在
            else:
                logger.warning(f"当天数据缺少列: {col}")
    
    # 确保trade_date是datetime类型
    if not pd.api.types.is_datetime64_any_dtype(daily_prepared['trade_date']):
        daily_prepared['trade_date'] = pd.to_datetime(daily_prepared['trade_date'], errors='coerce')
    
    # 过滤掉历史数据中与当天日期相同的数据（避免重复）
    trade_date_obj = pd.to_datetime(trade_date)
    historical_prepared = historical_prepared[
        historical_prepared['trade_date'] < trade_date_obj
    ]
    
    # 拼接数据
    # 选择需要的列
    common_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
    available_columns = [col for col in common_columns if col in historical_prepared.columns and col in daily_prepared.columns]
    
    historical_selected = historical_prepared[available_columns].copy()
    daily_selected = daily_prepared[available_columns].copy()
    
    # 合并
    merged_df = pd.concat([historical_selected, daily_selected], ignore_index=True)
    
    # 按日期排序
    merged_df = merged_df.sort_values('trade_date').reset_index(drop=True)
    
    return merged_df

