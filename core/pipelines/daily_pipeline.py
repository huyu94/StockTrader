"""
每日更新流水线

负责每日定期更新股票数据
"""

from typing import Any, Dict, List, Optional
from loguru import logger
import pandas as pd
from core.pipelines.base import BasePipeline
from core.collectors.base import BaseCollector
from core.transformers.base import BaseTransformer
from core.loaders.base import BaseLoader
from core.common.exceptions import PipelineException
from utils.date_helper import DateHelper





class DailyPipeline(BasePipeline):
    """
    每日更新流水线
    
    用于每日定期更新股票历史数据，通常采用增量更新模式
    更新内容包括：股票基本信息、交易日历、日K线数据、复权因子、前复权数据
    注意：实时K线数据更新已移至 StrategyPipeline
    """

    def __init__(self):
        super().__init__()


    
    def run(self, **kwargs) -> None:
        """
        执行每日更新流水线
        
        更新历史数据：股票基本信息、交易日历、日K线数据、复权因子、前复权数据
        
        Args:
            update_basic_info: 是否更新股票基本信息（默认 True）
            update_trade_calendar: 是否更新交易日历（默认 True）
            update_daily_kline: 是否更新日K线数据（默认 True）
            update_adj_factor: 是否更新复权因子（默认 True）
            update_qfq_data: 是否更新前复权数据（默认 True）
            **kwargs: 其他参数
        """
        trade_date = DateHelper.today()
        


        # 获取更新选项
        update_basic_info = kwargs.get("update_basic_info", True)
        update_trade_calendar = kwargs.get("update_trade_calendar", True)
        update_daily_kline = kwargs.get("update_daily_kline", True)
        update_adj_factor = kwargs.get("update_adj_factor", True)
        update_qfq_data = kwargs.get("update_qfq_data", True)

        if update_basic_info:
            logger.info("-" * 60)
            logger.info("步骤 1: 更新股票基本信息 (basic_info)")
            logger.info("-" * 60)
            self._update_basic_info()
        
        # 2. 更新 trade_calendar（交易日历）
        if update_trade_calendar:
            logger.info("-" * 60)
            logger.info("步骤 2: 更新交易日历 (trade_calendar)")
            logger.info("-" * 60)
            self._update_trade_calendar(trade_date)

        # 检查是否为交易日
        if not self._is_trading_day(trade_date):
            logger.info(f"今日 {trade_date} 不是交易日，跳过执行")
            return
        
        # 3. 更新 daily_kline（日K线数据）
        if update_daily_kline:
            logger.info("-" * 60)
            logger.info("步骤 3: 更新日K线数据 (daily_kline)")
            logger.info("-" * 60)
            self._update_daily_kline(trade_date)
        
        # 4. 更新 adj_factor（复权因子）
        if update_adj_factor:
            logger.info("-" * 60)
            logger.info("步骤 4: 更新复权因子 (adj_factor)")
            logger.info("-" * 60)
            self._update_adj_factor(trade_date)
        
        if update_qfq_data:
            logger.info("-" * 60)
            logger.info("步骤 5: 更新前复权数据 (qfq_data)")
            logger.info("-" * 60)
            self._update_qfq_data(trade_date)



    def _update_daily_kline(self, trade_date: str) -> None:
        """
        更新日K线数据
        """
        try:
            raw_data = self.daily_kline_collector.collect(trade_date=trade_date)
            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到日K线数据，日期:{trade_date}")
                return
            
            transformed_data = self.daily_kline_transformer.transform(raw_data)
            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的日K线数据为空，日期:{trade_date}")
                return
            
            self.daily_kline_loader.load(transformed_data, strategy=BaseLoader.LOAD_STRATEGY_APPEND)

        except Exception as e:
            logger.error(f"更新日K线数据失败，日期:{trade_date}，错误:{e}")
            raise PipelineException(f"更新日K线数据失败，日期:{trade_date}，错误:{e}") from e



    def _update_adj_factor(self, trade_date: str) -> None:
        """
        更新复权因子数据
        
        流程：
        1. 先爬取今天除权除息日，复权因子有变化的股票
        2. 将对应日期的，数据库里的这些股的复权因子都设置为对应复权因子，其他的沿用上一个日期的
        3. 然后写入数据库
        """
        try:
            # 1. 爬取今天除权除息日，复权因子有变化的股票
            ex_date_df = self.ex_date_collector.collect(ex_date=trade_date)
            if ex_date_df is None or ex_date_df.empty:
                logger.warning(f"未采集到除权除息日数据，日期:{trade_date}")
                return
            
            ts_codes = ex_date_df['ts_code'].unique().tolist()
            logger.info(f"发现 {len(ts_codes)} 只股票在 {trade_date} 除权除息")

            # 2. 收集新的复权因子数据
            new_adj_factors = []
            for ts_code in ts_codes:
                raw_data = self.adj_factor_collector.collect(ts_code=ts_code, trade_date=trade_date)
                
                if raw_data is None or raw_data.empty:
                    logger.warning(f"未采集到复权因子数据，股票:{ts_code}，日期:{trade_date}")
                    continue
                
                transformed_data = self.adj_factor_transformer.transform(raw_data)
                
                if transformed_data is None or transformed_data.empty:
                    logger.warning(f"转换后的复权因子数据为空，股票:{ts_code}，日期:{trade_date}")
                    continue

                new_adj_factors.append(transformed_data)
            
            if not new_adj_factors:
                logger.warning(f"未采集到复权因子数据，日期:{trade_date}")
                return
            
            # 合并新采集的复权因子数据
            new_adj_factor_df = pd.concat(new_adj_factors, ignore_index=True)
            
            # 3. 读取数据库中这些股票的历史复权因子数据
            # 获取所有需要更新的股票代码
            updated_ts_codes = new_adj_factor_df['ts_code'].unique().tolist()
            
            # 读取数据库中这些股票的历史复权因子
            existing_adj_factor_df = self.adj_factor_loader.read()
            
            if existing_adj_factor_df is None or existing_adj_factor_df.empty:
                # 如果数据库中没有历史数据，直接使用新数据
                logger.info("数据库中无历史复权因子数据，直接使用新采集的数据")
                final_adj_factor_df = new_adj_factor_df.copy()
            else:
                # 确保 trade_date 列是 datetime 类型以便比较
                if 'trade_date' in existing_adj_factor_df.columns:
                    existing_adj_factor_df['trade_date'] = pd.to_datetime(existing_adj_factor_df['trade_date'], errors='coerce')
                if 'trade_date' in new_adj_factor_df.columns:
                    new_adj_factor_df['trade_date'] = pd.to_datetime(new_adj_factor_df['trade_date'], errors='coerce')
                
                # 分离需要更新的股票和其他股票
                updated_stocks_mask = existing_adj_factor_df['ts_code'].isin(updated_ts_codes)
                other_stocks_df = existing_adj_factor_df[~updated_stocks_mask].copy()
                updated_stocks_df = existing_adj_factor_df[updated_stocks_mask].copy()
                
                # 对于需要更新的股票，移除今天日期的旧数据（如果有），然后添加新数据
                if not updated_stocks_df.empty:
                    trade_date_dt = pd.to_datetime(trade_date, errors='coerce')
                    # 移除今天日期的旧数据
                    updated_stocks_df = updated_stocks_df[
                        updated_stocks_df['trade_date'] != trade_date_dt
                    ]
                
                # 合并：其他股票的历史数据 + 需要更新股票的历史数据（已移除今天日期） + 新采集的数据
                final_adj_factor_df = pd.concat([
                    other_stocks_df,
                    updated_stocks_df,
                    new_adj_factor_df
                ], ignore_index=True)
                
                # 按股票代码和日期排序
                final_adj_factor_df = final_adj_factor_df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
            
            # 4. 写入数据库（使用 UPSERT 策略，确保更新而不是重复插入）
            self.adj_factor_loader.load(final_adj_factor_df, strategy=BaseLoader.LOAD_STRATEGY_UPSERT)
            logger.info(f"成功更新复权因子数据，共 {len(final_adj_factor_df)} 条记录")
            
        except Exception as e:
            logger.error(f"更新复权因子数据失败，日期:{trade_date}，错误:{e}")
            raise

            
    
    def _update_basic_info(self) -> None:
        """
        更新股票基本信息数据
        """
        try:
            raw_data = self.basic_info_collector.collect()
            
            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到股票基本信息数据")
                return
            
            transformed_data = self.basic_info_transformer.transform(raw_data)
            
            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的股票基本信息数据为空")
                return
            
            self.basic_info_loader.load(transformed_data, strategy=BaseLoader.LOAD_STRATEGY_UPSERT)
            
        except Exception as e:
            logger.error(f"更新股票基本信息数据失败，错误:{e}")
            raise

    def _update_trade_calendar(self, trade_date: str) -> None:
        """
        更新交易日历数据

        Args: 
            trade_date: 交易日 (YYYY-MM-DD)
        """
        try:
            logger.info(f"更新交易日历数据，日期:{trade_date}")

            raw_data = self.trade_calendar_collector.collect(start_date=trade_date, end_date=trade_date)

            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到交易日历数据，日期:{trade_date}")
                return
            
            transformed_data = self.trade_calendar_transformer.transform(raw_data)

            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的交易日历数据为空，日期:{trade_date}")
                return 

            self.trade_calendar_loader.load(transformed_data, strategy=BaseLoader.LOAD_STRATEGY_UPSERT)

        except Exception as e:
            logger.error(f"更新交易日历数据失败，日期:{trade_date}，错误:{e}")
            raise
        
    
    def _update_qfq_data(self, trade_date: str) -> None:
        """
        更新前复权数据
        
        流程：
        1. 获取除权除息日股票列表
        2. 对于除权除息日股票：重新计算所有历史前复权数据
        3. 对于非除权除息日股票：只更新当天数据，将未复权价格复制到前复权价格列
        """
        try:
            # 1. 获取除权除息日股票列表
            ex_date_df = self.ex_date_collector.collect(ex_date=trade_date)
            if ex_date_df is None or ex_date_df.empty:
                ex_ts_codes = set()
                logger.info(f"未发现除权除息日股票，日期:{trade_date}")
            else:
                ex_ts_codes = set(ex_date_df['ts_code'].unique().tolist())
                logger.info(f"发现 {len(ex_ts_codes)} 只股票在 {trade_date} 除权除息")
            
            # 2. 获取数据库中所有股票代码
            all_ts_codes = self.basic_info_loader.get_all_ts_codes()
            if not all_ts_codes:
                logger.warning(f"数据库中无股票代码，日期:{trade_date}")
                return
            
            # 3. 分离除权除息日股票和非除权除息日股票
            non_ex_ts_codes = [ts_code for ts_code in all_ts_codes if ts_code not in ex_ts_codes]
            
            logger.info(f"开始更新前复权数据，日期:{trade_date}")
            logger.info(f"  - 除权除息日股票: {len(ex_ts_codes)} 只（需重新计算所有历史数据）")
            logger.info(f"  - 非除权除息日股票: {len(non_ex_ts_codes)} 只（只更新当天数据）")
            
            ex_success_count = 0
            ex_fail_count = 0
            non_ex_success_count = 0
            non_ex_fail_count = 0
            
            # 4. 处理除权除息日股票（重新计算所有历史数据）
            if ex_ts_codes:
                logger.info("-" * 60)
                logger.info("处理除权除息日股票（重新计算所有历史前复权数据）")
                logger.info("-" * 60)
                
                for idx, ts_code in enumerate(ex_ts_codes, 1):
                    try:
                        # 读取该股票的所有历史复权因子数据
                        adj_factor_df = self.adj_factor_loader.read(ts_code=ts_code)
                        if adj_factor_df is None or adj_factor_df.empty:
                            logger.debug(f"未找到复权因子数据，股票:{ts_code}，日期:{trade_date}")
                            ex_fail_count += 1
                            continue
                        
                        # 读取该股票的所有历史日K线数据
                        daily_kline_df = self.daily_kline_loader.read(ts_code=ts_code)
                        if daily_kline_df is None or daily_kline_df.empty:
                            logger.debug(f"未找到日K线数据，股票:{ts_code}，日期:{trade_date}")
                            ex_fail_count += 1
                            continue
                        
                        # 使用 qfq_calculator 重新计算所有历史前复权数据
                        qfq_calculator_df = self.qfq_calculator.calculate(daily_kline_df, adj_factor_df)
                        if qfq_calculator_df is None or qfq_calculator_df.empty:
                            logger.warning(f"未计算出前复权数据，股票:{ts_code}，日期:{trade_date}")
                            ex_fail_count += 1
                            continue
                        
                        # 使用 UPSERT 策略写入数据库（更新所有历史数据）
                        self.daily_kline_loader.load(qfq_calculator_df, BaseLoader.LOAD_STRATEGY_UPSERT)
                        ex_success_count += 1
                        
                        # 每处理10只股票输出一次进度（除权除息日股票通常较少）
                        if idx % 10 == 0:
                            logger.info(f"除权除息日股票处理进度: {idx}/{len(ex_ts_codes)}，成功:{ex_success_count}，失败:{ex_fail_count}")
                    
                    except Exception as e:
                        logger.error(f"更新除权除息日股票前复权数据失败，股票:{ts_code}，日期:{trade_date}，错误:{e}")
                        ex_fail_count += 1
                        continue
                
                logger.info(f"除权除息日股票处理完成，成功:{ex_success_count}，失败:{ex_fail_count}")
            
            # 5. 处理非除权除息日股票（只更新当天数据）
            if non_ex_ts_codes:
                logger.info("-" * 60)
                logger.info("处理非除权除息日股票（只更新当天数据）")
                logger.info("-" * 60)
                
                for idx, ts_code in enumerate(non_ex_ts_codes, 1):
                    try:
                        # 只读取当天的日K线数据
                        daily_kline_df = self.daily_kline_loader.read(
                            ts_code=ts_code,
                            start_date=trade_date,
                            end_date=trade_date
                        )
                        if daily_kline_df is None or daily_kline_df.empty:
                            logger.debug(f"未找到当天日K线数据，股票:{ts_code}，日期:{trade_date}")
                            non_ex_fail_count += 1
                            continue
                        
                        # 将未复权价格复制到前复权价格列
                        qfq_df = daily_kline_df.copy()
                        qfq_df['close_qfq'] = qfq_df['close']
                        qfq_df['open_qfq'] = qfq_df['open']
                        qfq_df['high_qfq'] = qfq_df['high']
                        qfq_df['low_qfq'] = qfq_df['low']
                        
                        # 只更新当天的数据（使用 UPSERT 策略）
                        self.daily_kline_loader.load(qfq_df, BaseLoader.LOAD_STRATEGY_UPSERT)
                        non_ex_success_count += 1
                        
                        # 每处理100只股票输出一次进度
                        if idx % 100 == 0:
                            logger.info(f"非除权除息日股票处理进度: {idx}/{len(non_ex_ts_codes)}，成功:{non_ex_success_count}，失败:{non_ex_fail_count}")
                    
                    except Exception as e:
                        logger.error(f"更新非除权除息日股票前复权数据失败，股票:{ts_code}，日期:{trade_date}，错误:{e}")
                        non_ex_fail_count += 1
                        continue
                
                logger.info(f"非除权除息日股票处理完成，成功:{non_ex_success_count}，失败:{non_ex_fail_count}")
            
            # 6. 输出总体统计
            total_success = ex_success_count + non_ex_success_count
            total_fail = ex_fail_count + non_ex_fail_count
            logger.info("-" * 60)
            logger.info(f"前复权数据更新完成，日期:{trade_date}")
            logger.info(f"  - 除权除息日股票: 成功 {ex_success_count}，失败 {ex_fail_count}")
            logger.info(f"  - 非除权除息日股票: 成功 {non_ex_success_count}，失败 {non_ex_fail_count}")
            logger.info(f"  - 总计: 成功 {total_success}，失败 {total_fail}")
            logger.info("-" * 60)
            
        except Exception as e:
            logger.error(f"更新前复权数据失败，日期:{trade_date}，错误:{e}")
            raise PipelineException(f"更新前复权数据失败，日期:{trade_date}，错误:{e}") from e

    def _update_real_time_data(self, trade_date: str) -> None:
        """
        更新实时数据
        
        注意：此方法已废弃，实时数据更新功能已移至 StrategyPipeline
        保留此方法定义以便将来使用，但不再在 run 方法中调用
        """
        try:
            raw_data = self.intraday_kline_collector.collect()
            if raw_data is None or raw_data.empty:
                logger.warning(f"未采集到实时数据，日期:{trade_date}")
                return
            
            transformed_data = self.intraday_kline_transformer.transform(raw_data, trade_date=trade_date)
            if transformed_data is None or transformed_data.empty:
                logger.warning(f"转换后的实时数据为空，日期:{trade_date}")
                return

            self.intraday_kline_loader.load(transformed_data, strategy=BaseLoader.LOAD_STRATEGY_APPEND)

        except Exception as e:
            logger.error(f"更新实时数据失败，日期:{trade_date}，错误:{e}")
            raise
    
    def _is_trading_day(self, trade_date: str) -> bool:
        """
        检查指定日期是否为交易日
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)
            
        Returns:
            bool: 是否为交易日
        """
        try:
            # 查询交易日历
            calendar_df = self.trade_calendar_loader.read(cal_date=trade_date)
            
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