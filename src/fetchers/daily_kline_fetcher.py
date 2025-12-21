import os
import threading
from typing import Optional, List, Dict
import pandas as pd
from loguru import logger
from project_var import DATA_DIR
from src.providers import BaseProvider, TushareProvider
import dotenv

dotenv.load_dotenv()

class DailyKlineFetcher:
    def __init__(self, provider_name: str = "tushare", provider: Optional[BaseProvider] = None):
        self.provider_name = provider_name
        self.provider = provider or (TushareProvider() if provider_name == "tushare" else None)
        
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        # 统一存放在 stock_data 目录下，文件名为 {ts_code}.csv
        self.stock_data_dir = os.path.join(self.data_dir, "stock_data")
        os.makedirs(self.stock_data_dir, exist_ok=True)
        
        # 简单的文件锁，防止并发写入同一个文件（虽然目前策略是串行日期，但不同日期可能写同一个文件）
        # 不过如果是按日期顺序处理，通常不会冲突。为了安全起见，可以使用。
        # 但考虑到 5000+ 个文件，维护 5000 个锁开销太大。
        # 策略：Manager 层保证串行处理，或者接受极小概率的冲突。
        # 由于我们是 append 模式，且一次只写入一行（针对单股票），冲突概率低。
        # 最好的方式是：读取 -> 合并 -> 写入。
    
    def fetch_daily_by_date(self, trade_date: str) -> pd.DataFrame:
        """
        按日期获取全市场日线行情
        """
        logger.info(f"Fetching daily kline for date: {trade_date}...")
        df = self.provider.query("daily", trade_date=trade_date)
        if df is None or df.empty:
            logger.warning(f"No daily data found for date: {trade_date}")
            return pd.DataFrame()
        return df

    def save_daily_data_to_stock_files(self, df: pd.DataFrame):
        """
        将包含多只股票数据的 DataFrame 拆分并追加保存到对应的股票文件中
        """
        if df.empty:
            return

        # 按股票代码分组，虽然每组只有一行，但这样逻辑通用
        grouped = df.groupby("ts_code")
        
        # 统计
        count = 0
        total = len(grouped)
        
        for ts_code, group_df in grouped:
            try:
                path = os.path.join(self.stock_data_dir, f"{ts_code}.csv")
                
                # 统一字段顺序，防止追加时错位
                # 假设 group_df 已经包含了需要的列，Tushare 返回的列顺序可能固定
                # 最好读取现有文件获取 header，或者强制指定标准 header
                
                if os.path.exists(path):
                    # 追加模式
                    # 为了去重，必须读取 -> 合并 -> 去重 -> 写入
                    # 这是一个 IO 密集型操作
                    try:
                        old_df = pd.read_csv(path, dtype={"trade_date": str})
                        # 简单检查是否已存在该日期
                        new_dates = group_df["trade_date"].tolist()
                        if not old_df[old_df["trade_date"].isin(new_dates)].empty:
                            # 已存在，跳过或覆盖
                            # 这里选择：合并并去重，以新数据为准
                            merged = pd.concat([old_df, group_df], ignore_index=True)
                            merged = merged.drop_duplicates(subset=["trade_date"], keep="last")
                            merged = merged.sort_values("trade_date")
                            merged.to_csv(path, index=False, encoding="utf-8-sig")
                        else:
                            # 不存在，直接追加（为了保持有序，还是得读出来排个序...或者假设是追加最新日期）
                            # 如果确认是按日期递增抓取，可以直接追加到文件末尾
                            # 但为了健壮性，建议还是 full rewrite
                            merged = pd.concat([old_df, group_df], ignore_index=True)
                            merged = merged.sort_values("trade_date")
                            merged.to_csv(path, index=False, encoding="utf-8-sig")
                    except Exception as e:
                        logger.error(f"Error reading/writing {ts_code}: {e}")
                else:
                    # 新文件
                    group_df.to_csv(path, index=False, encoding="utf-8-sig")
                
                count += 1
                # 每处理 100 个打印一下进度？太频繁了，交给外层进度条吧
                
            except Exception as e:
                logger.error(f"Failed to save data for {ts_code}: {e}")

        logger.info(f"Saved {count}/{total} stocks for this date.")
