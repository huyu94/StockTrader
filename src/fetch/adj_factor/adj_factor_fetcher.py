import os
from typing import Optional
from datetime import timedelta
import pandas as pd
from loguru import logger
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from src.fetch.providers import BaseProvider, TushareProvider
from utils.date_helper import DateHelper
from src.storage.adj_factor_storage_mysql import AdjFactorStorageMySQL


class AdjFactorFetcher:
    """
    这里应该要分两段，一段是爬取除权除息日，然后爬取对应除权除息日的股票的复权因子
    """
    def __init__(self, 
        provider: BaseProvider,
        storage: AdjFactorStorageMySQL,
        max_write_workers: int = 2  # 写入线程池大小
        ):
        self.provider = provider
        self.storage = storage
        # 创建写入线程池
        self.write_executor = ThreadPoolExecutor(max_workers=max_write_workers, thread_name_prefix="WriteWorker")
        self.pending_writes = []  # 存储待完成的写入任务
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'write_executor'):
            self.write_executor.shutdown(wait=True)




    def _fetch_ex_date(self, ex_date: str) -> pd.DataFrame:
        """
        爬取除权除息日
        """
        params = {}
        params['ex_date'] = ex_date
        params['fields'] = 'ts_code, ex_date'
        df = self.provider.query("dividend", **params)
        return df

    def _fetch_adj_factor(self, ts_codes: list[str], trade_date: str) -> pd.DataFrame:
        """
        """
        params = {}
        # params['ts_code'] = ts_code
        params['trade_date'] = trade_date
        params['fields'] = 'ts_code, trade_date, adj_factor'
        df = self.provider.query("adj_factor", **params)
        return df[df.ts_code.isin(ts_codes)]
    
    def _write_task(self, df: pd.DataFrame, description: str = "") -> None:
        """
        写入任务（实例方法，避免每次创建闭包）
        
        Args:
            df: 要写入的 DataFrame
            description: 任务描述（用于日志）
        """
        try:
            df_storage = self.to_storage_format(df)
            # 异步写入时禁用进度条，避免多个进度条同时显示
            self.storage.write(df_storage, show_progress=False)
            logger.debug(f"异步写入完成: {description}, {len(df)} 条记录")
        except Exception as e:
            logger.error(f"异步写入失败: {description}, 错误: {e}")
    
    def _write_async(self, df: pd.DataFrame, description: str = "") -> None:
        """
        异步写入数据到数据库
        
        Args:
            df: 要写入的 DataFrame
            description: 任务描述（用于日志）
        """
        if df is None or df.empty:
            return
        
        # 提交写入任务到线程池（使用实例方法，避免每次创建闭包）
        future = self.write_executor.submit(self._write_task, df, description)
        self.pending_writes.append((future, description))
    
    def _wait_all_writes(self) -> None:
        """等待所有写入任务完成"""
        if not self.pending_writes:
            return
        
        logger.info(f"等待 {len(self.pending_writes)} 个写入任务完成...")
        with tqdm(total=len(self.pending_writes), desc="写入数据", unit="批", leave=False) as pbar:
            for future, description in self.pending_writes:
                try:
                    future.result()  # 等待任务完成
                    pbar.update(1)
                except Exception as e:
                    logger.error(f"写入任务失败: {description}, 错误: {e}")
                    pbar.update(1)
        
        # 清空待完成任务列表
        self.pending_writes.clear()
        logger.info("所有写入任务已完成")
        


    def fetch(self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        async_write: bool = False
        ) -> pd.DataFrame:
        """
        查询除权除息日的复权因子数据
        
        :param start_date: 开始日期（YYYYMMDD）
        :param end_date: 结束日期（YYYYMMDD）
        :param async_write: 是否异步写入（如果为True，每天的数据会异步写入）
        :return: DataFrame，包含字段 trade_date, ts_code, ex_date, adj_factor
        """
        if not start_date or not end_date:
            raise ValueError("start_date 和 end_date 必须同时提供")
        
        # 标准化日期格式
        start_date_normalized = DateHelper.normalize_to_yyyymmdd(start_date)
        end_date_normalized = DateHelper.normalize_to_yyyymmdd(end_date)
        
        # 转换为 date 对象用于遍历
        start = DateHelper.parse_to_date(start_date_normalized)
        end = DateHelper.parse_to_date(end_date_normalized)
        
        if start > end:
            raise ValueError("start_date 不能大于 end_date")
        
        # 计算总天数
        total_days = (end - start).days + 1
        
        # 存储所有结果
        all_results = []
        
        # 遍历日期范围，使用 tqdm 显示进度
        current_date = start
        with tqdm(total=total_days, desc="查询除权除息数据", unit="天") as pbar:
            while current_date <= end:
                # 转换为 YYYYMMDD 格式用于 API 调用
                trade_date_str = current_date.strftime('%Y%m%d')
                
                # 更新进度条描述
                pbar.set_description(f"查询日期 {current_date.strftime('%Y-%m-%d')}")
                
                # 获取该日期的除权除息股票数据
                ex_date_df = self._fetch_ex_date(trade_date_str)
                ts_codes = ex_date_df['ts_code'].tolist()
                adj_factor_df = self._fetch_adj_factor(ts_codes, trade_date_str)
                result_df = adj_factor_df[adj_factor_df['ts_code'].isin(ts_codes)]
                
                if not result_df.empty:
                    if async_write:
                        # 异步写入当天的数据
                        self._write_async(result_df, f"日期 {current_date.strftime('%Y-%m-%d')}")
                    else:
                        all_results.append(result_df)
                
                # 移动到下一天
                current_date += timedelta(days=1)
                pbar.update(1)
        
        if async_write:
            return pd.DataFrame()  # 异步写入模式，不返回数据
        else:
            # 转换为 DataFrame
            if all_results:
                result_df = pd.concat(all_results).reset_index(drop=True)
                logger.info(f"共获取 {len(result_df)} 条复权因子数据")
                return result_df
            else:
                logger.info("未找到任何复权因子数据")
                return pd.DataFrame(columns=['trade_date', 'ts_code', 'ex_date', 'adj_factor']) 

    def to_storage_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        """
        return df

    def update(self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        async_write: bool = True  # 默认启用异步写入
        ):
        """
        更新复权因子数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            async_write: 是否异步写入（默认True，边爬取边写入，提高效率）
        """
        try:
            # 清空之前的待完成任务
            self.pending_writes.clear()
            
            # 开始爬取（如果 async_write=True，数据会边爬取边写入）
            df = self.fetch(start_date, end_date, async_write=async_write)
            
            if async_write:
                # 异步写入模式：等待所有写入任务完成
                self._wait_all_writes()
            else:
                # 同步写入模式：统一写入所有数据
                if not df.empty:
                    df_storage = self.to_storage_format(df)
                    self.storage.write(df_storage)
        finally:
            # 确保所有任务完成（即使出错也要等待）
            if self.pending_writes:
                self._wait_all_writes()