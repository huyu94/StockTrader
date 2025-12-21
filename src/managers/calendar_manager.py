from typing import Optional, List
import pandas as pd
from datetime import timedelta
from functools import cached_property
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from src.loaders.calendar_loader import CalendarLoader
from src.fetchers.calendar_fetcher import CalendarFetcher

class CalendarManager:
    """交易日历管理器
    协调Loader和Fetcher，实现自动更新与缓存读取
    """
    def __init__(self, provider_name: str = "tushare"):
        self.loader = CalendarLoader()
        self.fetcher = CalendarFetcher(provider_name=provider_name)
        
    def _get_calendar(self, exchange: str) -> pd.DataFrame:
        if self.loader.check_update_needed(exchange):
            logger.info(f"Updating calendar for {exchange}...")
            
            # 强制指定文件名以符合 Loader 的预期
            filename = f"{exchange}_trade_calendar.csv"
            
            # 计算默认日期范围：近一年
            now = pd.Timestamp.now()
            end_date = now.strftime("%Y%m%d")
            start_date = (now - timedelta(days=365)).strftime("%Y%m%d")
            
            # fetcher 会负责拉取、保存文件、更新 cache
            df = self.fetcher.fetch(start_date=start_date, end_date=end_date, exchange=exchange, filename=filename, save_local=True)
            return df
        else:
            logger.debug(f"Loading calendar from local for {exchange}")
            return self.loader.load(exchange)

    @cached_property
    def sse_calendar(self) -> pd.DataFrame:
        """获取上交所交易日历"""
        return self._get_calendar("SSE")

    @cached_property
    def szse_calendar(self) -> pd.DataFrame:
        """获取深交所交易日历"""
        return self._get_calendar("SZSE")
    
    def get_merged_calendar(self, exchanges: List[str] = None) -> pd.DataFrame:
        """
        获取合并后的交易日历 (宽表格式)
        Index: cal_date
        Columns: [exchange1, exchange2, ...]，值为bool表示是否开市
        
        注意：使用 outer join 取并集。
        如果某一方没有数据，则认为该日不开市 (False)。
        """
        if exchanges is None:
            exchanges = ["SSE", "SZSE"]
            
        dfs = []
        
        # 使用多线程并发获取各交易所日历
        with ThreadPoolExecutor(max_workers=min(len(exchanges), 5)) as executor:
            future_to_exchange = {executor.submit(self._get_calendar, exchange): exchange for exchange in exchanges}
            
            # 这里不需要顺序，但最终合并时顺序由 concat 处理，或者稍后排序
            # 我们按照完成顺序收集，最后统一处理
            results = {}
            for future in as_completed(future_to_exchange):
                exchange = future_to_exchange[future]
                try:
                    df = future.result()
                    results[exchange] = df
                except Exception as e:
                    logger.error(f"Error fetching calendar for {exchange}: {e}")

        # 保持输入顺序处理结果
        for exchange in exchanges:
            df = results.get(exchange)
            if df is not None and not df.empty:
                # 假设df列为 [exchange, cal_date, is_open]
                # 重命名 is_open 为对应的 exchange 名称
                # 将 is_open (1/0) 转为 boolean
                df = df.copy()
                df[exchange] = df["is_open"].astype(bool)
                # 只保留 cal_date 和 转换后的列
                df = df[["cal_date", exchange]]
                # 设置 cal_date 为索引以便后续合并
                df = df.set_index("cal_date")
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
            
        # 使用 outer join 取并集，保留所有日期
        merged = pd.concat(dfs, axis=1, join="outer").sort_index()
        
        # 填充 NaN 为 False (假设某日历没数据即为不开市)
        merged = merged.fillna(False).infer_objects(copy=False)
        
        return merged

    @cached_property
    def calendar(self) -> pd.DataFrame:
        """默认返回 SSE 和 SZSE 的合并日历"""
        logger.info(f"目前calendar包含SSE和SZSE的合并日历")
        return self.get_merged_calendar(["SSE", "SZSE"])
