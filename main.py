from core.pipelines.history_pipeline import HistoryPipeline
from utils.setup_logger import setup_logger

setup_logger()
# 创建流水线实例
pipeline = HistoryPipeline()

# 执行历史数据补全
pipeline.run(
    stock_codes=None,
    start_date="2015-01-01",
    end_date="2026-01-01",
    update_basic_info=True,      # 可选，默认 True
    update_trade_calendar=True,  # 可选，默认 True
    update_daily_kline=True,     # 可选，默认 True
    update_adj_factor=True        # 可选，默认 True
)

