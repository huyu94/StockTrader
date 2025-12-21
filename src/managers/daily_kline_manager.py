import pandas as pd
from tqdm import tqdm
from loguru import logger
from src.common.data_matrix_manager import DataMatrixManager
from src.fetchers.daily_kline_fetcher import DailyKlineFetcher

class DailyKlineManager:
    """日线行情管理器
    策略：
    1. 加载数据存在性矩阵 (DataMatrix)
    2. 统计每天的缺失股票数量
    3. 如果某天缺失 > 1000，则调用 Fetcher 按日期全量拉取，并追加到本地文件
    4. 更新矩阵缓存
    """
    def __init__(self):
        self.matrix_manager = DataMatrixManager()
        self.fetcher = DailyKlineFetcher()
        self.missing_threshold = 1000

    def update_all(self, start_date: str = "20100101"):
        """
        执行全量更新检查
        """
        # 1. 获取/生成矩阵
        # 先尝试加载缓存
        matrix = self.matrix_manager.load_matrix()
        if matrix.empty:
            logger.info("Matrix cache missing or empty, generating...")
            matrix = self.matrix_manager.generate_matrix(start_date=start_date)
            
        if matrix.empty:
            logger.error("Failed to obtain data matrix.")
            return

        # 2. 检查日期范围
        # matrix index 是 YYYYMMDD 或 YYYY-MM-DD (取决于CalendarManager，通常是 YYYYMMDD)
        # 需要确保和 Tushare 入参一致 (YYYYMMDD)
        # CalendarManager 返回的 cal_date 是 YYYYMMDD 字符串
        
        # 过滤出 start_date 之后的日期
        target_dates = [d for d in matrix.index if d >= start_date]
        target_dates.sort()
        
        logger.info(f"Checking {len(target_dates)} trading days for missing data...")
        
        # 3. 遍历日期
        for trade_date in tqdm(target_dates, desc="Checking daily data"):
            # 获取该日所有股票的存在状态
            # matrix.loc[trade_date] 是一个 Series (index=ts_code, value=bool)
            row = matrix.loc[trade_date]
            missing_count = (~row).sum() # False 的数量
            
            # 如果缺失数量超过阈值
            if missing_count > self.missing_threshold:
                logger.info(f"Date {trade_date}: Missing {missing_count} stocks (> {self.missing_threshold}). Fetching full daily data...")
                
                # Fetch
                df = self.fetcher.fetch_daily_by_date(trade_date)
                
                if not df.empty:
                    # Save (Split and append)
                    self.fetcher.save_daily_data_to_stock_files(df)
                    
                    # Update matrix in memory (optional, but good for consistency)
                    # 假设 fetch 到的 ts_code 都已保存成功
                    fetched_codes = df["ts_code"].unique()
                    # 只更新那些原本缺失的
                    # 注意：fetcher 可能返回了 matrix 中没有的新股，或者 matrix 中有但 fetcher 没返回的停牌股
                    valid_codes = matrix.columns.intersection(fetched_codes)
                    matrix.loc[trade_date, valid_codes] = True
                    
                    # 每次拉取完保存一次矩阵？还是最后保存？
                    # 为了防止中断，可以每隔一段时间保存一次，或者每次都保存（pickling 开销不大）
                    # 这里选择每次成功拉取后保存，防止白跑
                    try:
                        matrix.to_pickle(self.matrix_manager.cache_path)
                    except Exception as e:
                        logger.error(f"Failed to save matrix cache: {e}")
                else:
                    logger.warning(f"Date {trade_date}: Fetched empty data.")
            else:
                # logger.debug(f"Date {trade_date}: Missing {missing_count} stocks. Skiping.")
                pass
                
        logger.info("Daily kline update check completed.")
