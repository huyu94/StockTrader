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





class DailyPipeline(BasePipeline):
    """
    每日更新流水线
    
    用于每日定期更新股票数据，通常采用增量更新模式
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)


    
    def run(self, stock_codes: List[str], start_date: str = None, end_date: str = None, **kwargs) -> None:
        """
        执行每日更新流水线
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)，默认使用最近一个交易日
            end_date: 结束日期 (YYYY-MM-DD)，默认使用当前日期
            **kwargs: 其他参数
        """
        logger.info(f"执行每日更新流水线，股票数量: {len(stock_codes)}")
        raise NotImplementedError("DailyPipeline.run 方法待实现")


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
            
            self.daily_kline_loader.load(transformed_data)

        except Exception as e:
            logger.error(f"更新日K线数据失败，日期:{trade_date}，错误:{e}")
            raise



    def _update_adj_factor(self, trade_date: str) -> None:
        """
        更新复权因子数据
        """
        try:
            ex_date_df = self.ex_date_collector.collect(trade_date=trade_date)
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
                self.adj_factor_loader.load(merged_data)
            else:
                logger.warning(f"未采集到复权因子数据，日期:{trade_date}")
                return
            
        except Exception as e:
            logger.error(f"更新复权因子数据失败，日期:{trade_date}，错误:{e}")
            raise

            
    
    def _update_basic_info(self, ts_codes: List[str]) -> None:
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
            
            self.basic_info_loader.load(transformed_data)
            
        except Exception as e:
            logger.error(f"更新股票基本信息数据失败，股票数量: {len(ts_codes)}，错误:{e}")
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

            self.trade_calendar_loader.load(transformed_data)

        except Exception as e:
            logger.error(f"更新交易日历数据失败，日期:{trade_date}，错误:{e}")
            raise
        


