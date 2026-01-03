from core.pipelines import daily_pipeline
from core.pipelines.history_pipeline import HistoryPipeline
from core.pipelines.daily_pipeline import DailyPipeline
from utils.setup_logger import setup_logger

setup_logger()


def update_history():
    
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


def update_daily():
    daily_pipeline = DailyPipeline()
    daily_pipeline.run(
        update_basic_info=True,
        update_trade_calendar=True,
        update_daily_kline=True,
        update_adj_factor=True,
        update_qfq_data=True,
        update_real_time_data=True
    )


if __name__ == "__main__":
    update_daily()