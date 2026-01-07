"""
单股票历史数据补充脚本

用于补充单条股票的历史数据（日K线、复权因子、前复权数据）
"""

import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.pipelines.history_pipeline import HistoryPipeline
from utils.setup_logger import setup_logger

# 配置参数
TS_CODE = "603828.SH"  # 股票代码
START_DATE = "2015-01-01"  # 开始日期
END_DATE = "2026-01-07"  # 结束日期

TS_CODES_POOL = ['002049.SZ','002969.SZ','605255.SH','600058.SH','300658.SZ','000670.SZ','300087.SZ','920641.BJ','603778.SH','920680.BJ']

def main():
    """
    执行单股票历史数据补充
    """
    try:
        setup_logger()
        
        # 执行单股票历史数据补全
        # 注意：每次循环创建新的 pipeline 实例，因为 run_single_stock 会在 finally 中关闭线程池
        for ts_code in TS_CODES_POOL:
            logger.info(f"开始处理股票: {ts_code}")
            pipeline = HistoryPipeline()
            try:
                pipeline.run_single_stock(
                    ts_code=ts_code,
                    start_date=START_DATE,
                    end_date=END_DATE
                )
                logger.info(f"股票 {ts_code} 处理完成")
            except Exception as e:
                logger.error(f"处理股票 {ts_code} 失败: {e}")
                # 继续处理下一个股票
                continue
        
    except Exception as e:
        logger.error(f"单股票历史数据补充失败: {e}")
        raise


if __name__ == "__main__":
    main()

