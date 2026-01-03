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
    
    用于每日定期更新股票数据，通常采用增量更新模式
    """

    def __init__(self):
        super().__init__()


    
    def run(self, **kwargs) -> None:
        """
        执行每日更新流水线
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)，默认使用最近一个交易日
            end_date: 结束日期 (YYYY-MM-DD)，默认使用当前日期
            **kwargs: 其他参数
        """
        trade_date = DateHelper.today()
        


        # 获取更新选项
        update_basic_info = kwargs.get("update_basic_info", True)
        update_trade_calendar = kwargs.get("update_trade_calendar", True)
        update_daily_kline = kwargs.get("update_daily_kline", True)
        update_adj_factor = kwargs.get("update_adj_factor", True)
        update_qfq_data = kwargs.get("update_qfq_data", True)
        update_real_time_data = kwargs.get("update_real_time_data", True)

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

        # 6. 更新实时数据
        if update_real_time_data:
            logger.info("-" * 60)
            logger.info("步骤 6: 更新实时数据 (real_time_data)")
            logger.info("-" * 60)
            self._update_real_time_data(trade_date)



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
        """
        try:
            ex_date_df = self.ex_date_collector.collect(ex_date=trade_date)
            if ex_date_df is None or ex_date_df.empty:
                logger.warning(f"未采集到除权除息日数据，日期:{trade_date}")
                return
            
            ts_codes = ex_date_df['ts_code'].unique().tolist()

            all_results = []
            for ts_code in ts_codes:
                raw_data = self.adj_factor_collector.collect(ts_code=ts_code, trade_date=trade_date)
                
                if raw_data is None or raw_data.empty:
                    logger.warning(f"未采集到复权因子数据，股票:{ts_code}，日期:{trade_date}")
                    continue
                
                transformed_data = self.adj_factor_transformer.transform(raw_data)
                
                if transformed_data is None or transformed_data.empty:
                    logger.warning(f"转换后的复权因子数据为空，股票:{ts_code}，日期:{trade_date}")
                    continue

                all_results.append(transformed_data)
            
            if all_results:
                merged_data = pd.concat(all_results, ignore_index=True)
                self.adj_factor_loader.load(merged_data, strategy=BaseLoader.LOAD_STRATEGY_APPEND)
            else:
                logger.warning(f"未采集到复权因子数据，日期:{trade_date}")
                return
            
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
        ex_date_df = self.ex_date_collector.collect(ex_date=trade_date)
        if ex_date_df is None or ex_date_df.empty:
            logger.warning(f"未采集到除权除息日数据，日期:{trade_date}")
            return
        
        ts_codes = ex_date_df['ts_code'].unique().tolist()
        
        for ts_code in ts_codes:
            adj_factor_df = self.adj_factor_loader.read(ts_code=ts_code)
            if adj_factor_df is None or adj_factor_df.empty:
                logger.warning(f"未采集到复权因子数据，股票:{ts_code}，日期:{trade_date}")
                continue
            daily_kline_df = self.daily_kline_loader.read(ts_code=ts_code)
            if daily_kline_df is None or daily_kline_df.empty:
                logger.warning(f"未采集到日K线数据，股票:{ts_code}，日期:{trade_date}")
                continue
            qfq_calculator_df = self.qfq_calculator.calculate(daily_kline_df, adj_factor_df)
            if qfq_calculator_df is None or qfq_calculator_df.empty:
                logger.warning(f"未计算出前复权数据，股票:{ts_code}，日期:{trade_date}")
                continue
            
            self.daily_kline_loader.load(qfq_calculator_df, BaseLoader.LOAD_STRATEGY_UPSERT)

    def _update_real_time_data(self, trade_date: str) -> None:
        """
        更新实时数据
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