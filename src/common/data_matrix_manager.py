import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from loguru import logger
from project_var import DATA_DIR, CACHE_DIR
from src.managers.calendar_manager import CalendarManager
from src.managers.basic_info_manager import BasicInfoManager

class DataMatrixManager:
    """数据存在性矩阵管理器
    生成并维护一个 [交易日期 x 股票代码] 的布尔矩阵，
    表示本地是否已下载该日该股票的数据。
    """
    def __init__(self):
        self.calendar_manager = CalendarManager()
        self.basic_info_manager = BasicInfoManager()
        self.cache_path = os.path.join(CACHE_DIR, "data_existence_matrix.pkl")
        self.stock_data_dir = os.path.join(DATA_DIR, "stock_data")
        os.makedirs(self.stock_data_dir, exist_ok=True)

    def generate_matrix(self, start_date: str = "20100101", end_date: str = None) -> pd.DataFrame:
        """
        生成数据存在性矩阵
        :param start_date: 开始日期 YYYYMMDD
        :param end_date: 结束日期 YYYYMMDD (默认今天)
        """
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"Preparing to generate data matrix ({start_date} - {end_date})...")

        # 1. 获取交易日期 (基于合并日历)
        calendar_df = self.calendar_manager.calendar
        # 筛选在范围内且至少有一个市场开市的日期 (假设calendar index是日期)
        # CalendarManager 返回的 calendar index 是 cal_date (str)
        # 且包含 SSE, SZSE 等列 (bool)
        # 我们只关心是否有交易，只要任一交易所开市即可，或者干脆取所有日历日期（通常非交易日也没有数据）
        # 这里取所有日历中的日期作为索引，后续如果需要只看交易日可以再过滤
        
        all_dates = calendar_df.index
        target_dates = all_dates[(all_dates >= start_date) & (all_dates <= end_date)]
        target_dates = sorted(target_dates.tolist())
        
        if not target_dates:
            logger.warning("No dates found in calendar range.")
            return pd.DataFrame()

        # 2. 获取所有股票代码
        stocks = self.basic_info_manager.all_stock_codes
        if not stocks:
            logger.warning("No stock codes found.")
            return pd.DataFrame()

        logger.info(f"Matrix size: {len(target_dates)} dates x {len(stocks)} stocks")

        # 3. 初始化矩阵 (默认False)
        # 使用 bool 类型节省内存
        matrix = pd.DataFrame(False, index=target_dates, columns=stocks, dtype=bool)

        # 4. 扫描本地文件
        # 定义单个文件处理函数
        def check_stock_file(ts_code):
            path = os.path.join(self.stock_data_dir, f"{ts_code}.csv")
            if not os.path.exists(path):
                return ts_code, []
            
            try:
                # 只读取 trade_date 列
                # 假设文件编码为 utf-8-sig (StockLoader中提到)
                # 使用 chunksize 或者是只读 header? 不，我们需要所有日期。
                # usecols 可以减少内存
                df = pd.read_csv(path, usecols=["trade_date"], encoding="utf-8-sig", dtype={"trade_date": str})
                if df.empty:
                    return ts_code, []
                
                # 统一日期格式 YYYYMMDD
                # 如果文件中是 2025-01-01 格式，需要转换。Tushare daily 通常是 YYYYMMDD。
                # 尝试转换一下以防万一
                dates = df["trade_date"].tolist()
                # 简单清洗：如果包含 '-' 则去掉
                cleaned_dates = [d.replace("-", "") for d in dates if isinstance(d, str)]
                return ts_code, cleaned_dates
            except Exception:
                # 文件损坏或格式不对
                return ts_code, []

        # 并发读取
        # 此时是 IO 密集型，可以使用较多线程
        max_workers = 20
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_code = {executor.submit(check_stock_file, code): code for code in stocks}
            
            for future in tqdm(as_completed(future_to_code), total=len(stocks), desc="Scanning local data"):
                ts_code = future_to_code[future]
                try:
                    code, present_dates = future.result()
                    if present_dates:
                        # 过滤出在矩阵索引范围内的日期
                        # 使用 isin 批量设置
                        # 注意：present_dates 可能包含不在 target_dates 中的日期（如更早的），忽略即可
                        # 找出交集
                        valid_dates = matrix.index.intersection(present_dates)
                        if not valid_dates.empty:
                            matrix.loc[valid_dates, code] = True
                except Exception as e:
                    logger.error(f"Error processing {ts_code}: {e}")

        # 5. 保存缓存
        try:
            matrix.to_pickle(self.cache_path)
            logger.info(f"Data matrix saved to {self.cache_path}")
        except Exception as e:
            logger.error(f"Failed to save data matrix: {e}")

        return matrix

    def load_matrix(self) -> pd.DataFrame:
        """从缓存加载矩阵"""
        if os.path.exists(self.cache_path):
            try:
                return pd.read_pickle(self.cache_path)
            except Exception as e:
                logger.error(f"Failed to load data matrix cache: {e}")
        return pd.DataFrame()
