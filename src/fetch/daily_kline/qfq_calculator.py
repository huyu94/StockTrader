"""
前复权计算器

用于读取数据库中的daily_kline数据和复权因子数据，计算前复权价格并更新到数据库。
支持单只股票和全市场批量计算，支持全量和增量更新模式。
"""
import pandas as pd
import numpy as np
from typing import Optional, List, Union
from loguru import logger
from decimal import Decimal
from tqdm import tqdm

from src.storage.daily_kline_storage_mysql import DailyKlineStorageMySQL
from src.storage.adj_factor_storage_mysql import AdjFactorStorageMySQL
from src.storage.orm_models import DailyKlineORM
from utils.date_helper import DateHelper
from config import setup_logger
setup_logger()

class QFQCalculator:
    """前复权计算器
    
    功能：
    1. 读取daily_kline数据（未复权价格）
    2. 读取复权因子数据
    3. 计算前复权价格
    4. 更新到数据库
    
    前复权计算公式：
    前复权价(历史日期T) = 未复权价(T) × 最新复权因子 / 历史复权因子(T)
    """
    
    def __init__(self, daily_storage: DailyKlineStorageMySQL, adj_storage: AdjFactorStorageMySQL):
        """
        初始化前复权计算器
        
        Args:
            daily_storage: 日线数据存储管理器
            adj_storage: 复权因子存储管理器
        """
        self.daily_storage = daily_storage
        self.adj_storage = adj_storage
    

    def calculate_qfq_prices(self , kline_df: pd.DataFrame, adj_factor_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算前复权价
        
        前复权计算公式：前复权价(T) = 未复权价(T) × 最新复权因子 / 历史复权因子(T)
        
        Args:
            kline_df: 日线数据DataFrame，每个交易日一行，包含 trade_date, open, high, low, close 等列
            adj_factor_df: 复权因子DataFrame，只有复权因子变化的那天有数据，包含 trade_date, adj_factor 列
        
        Returns:
            更新后的DataFrame，包含 adj_factor 列和前复权价格列（close_qfq, open_qfq, high_qfq, low_qfq）
        """
        # 复制数据，避免修改原DataFrame
        result_df = kline_df.copy()

        # 检查 adj_factor_df 是否为 None 或空
        if adj_factor_df is None or adj_factor_df.empty:
            logger.warning("复权因子数据为空，无法计算前复权价格")
            return result_df
        # 确保trade_date是日期类型（统一转换为datetime）
        if not pd.api.types.is_datetime64_any_dtype(result_df['trade_date']):
            result_df['trade_date'] = pd.to_datetime(result_df['trade_date'], errors='coerce')

        if not pd.api.types.is_datetime64_any_dtype(adj_factor_df['trade_date']):
            adj_factor_df = adj_factor_df.copy()  # 避免修改原DataFrame
            adj_factor_df['trade_date'] = pd.to_datetime(adj_factor_df['trade_date'], errors='coerce')


        # 按日期排序
        result_df = result_df.sort_values('trade_date').reset_index(drop=True)
        adj_factor_df = adj_factor_df.sort_values('trade_date').reset_index(drop=True)
        
        # 检查是否有复权因子数据
        if adj_factor_df.empty:
            logger.warning("复权因子数据为空，无法计算前复权价格")
            return result_df

        # 为每个交易日找到对应的复权因子（≤ 该日期的最近复权因子）
        # 使用 merge_asof 进行向前填充
        result_df = pd.merge_asof(
            result_df,
            adj_factor_df[['trade_date', 'adj_factor']],
            on='trade_date',
            direction='backward'  # 向后查找，找到 ≤ trade_date 的最近复权因子
        ).copy()  # 确保是独立副本，避免视图问题



        # 检查是否有交易日早于最早的复权因子日期
        earliest_adj_date = adj_factor_df['trade_date'].min()
        missing_dates = result_df[result_df['trade_date'] < earliest_adj_date]
        if not missing_dates.empty:
            logger.warning(
                f"有 {len(missing_dates)} 个交易日早于最早的复权因子日期 {earliest_adj_date.strftime('%Y-%m-%d')}，"
                f"这些交易日的前复权价格将无法计算"
            )

        # 获取最新复权因子（所有复权因子中日期最大的）
        latest_adj_factor = adj_factor_df.loc[adj_factor_df['trade_date'].idxmax(), 'adj_factor']
        if pd.isna(latest_adj_factor) or latest_adj_factor <= 0:
            raise ValueError(f"最新复权因子无效: {latest_adj_factor}")


        result_df['ratio'] = result_df['adj_factor'].apply(lambda x: latest_adj_factor / x if x > 0 else np.nan)
        result_df['close_qfq'] = result_df['close'] * result_df['ratio']
        result_df['open_qfq'] = result_df['open'] * result_df['ratio']
        result_df['high_qfq'] = result_df['high'] * result_df['ratio']
        result_df['low_qfq'] = result_df['low'] * result_df['ratio']


        return result_df

    def _load_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        加载复权因子数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        return self.adj_storage.load(ts_code=ts_code, start_date=start_date, end_date=end_date)
    
    def _load_daily_kline(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        加载日线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        return self.daily_storage.load(start_date=start_date, end_date=end_date, ts_codes=ts_code)
    
    def update_single_qfq(self, ts_code: str, start_date: str, end_date: str):
        """
        更新单只股票的前复权数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        kline_df = self._load_daily_kline(ts_code=ts_code, start_date=start_date, end_date=end_date)
        adj_factor_df = self._load_adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        result_df = self.calculate_qfq_prices(kline_df=kline_df, adj_factor_df=adj_factor_df)
        logger.info(f"更新股票 {ts_code} 的前复权数据完成，共 {len(result_df)} 条记录")
        self.write_to_database(result_df)
    
    def update_all_qfq(self, start_date: str, end_date: str, ts_codes: Union[List[str], str] = None):
        """
        更新全市场股票的前复权数据
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        """
        if ts_codes is None:
            ts_codes = self.get_ex_stock_codes(start_date=start_date, end_date=end_date)
        elif isinstance(ts_codes, str):
            ts_codes = [ts_codes]
        elif isinstance(ts_codes, list):
            pass
        

        for ts_code in tqdm(ts_codes, desc="更新前复权数据", total=len(ts_codes)):
            self.update_single_qfq(ts_code=ts_code, start_date=start_date, end_date=end_date)


    def get_ex_stock_codes(self, start_date: str, end_date: str) -> List[str]:
        """
        获取数据库里日期范围内除权除息的股票代码
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        return self.adj_storage.get_ex_stock_codes(start_date=start_date, end_date=end_date)


    def write_to_database(self, result_df: pd.DataFrame):
        """
        将前复权数据写入数据库
        
        Args:
            result_df: 前复权数据DataFrame
        """
        if result_df.empty:
            logger.warning("result_df 为空，没有数据可写入")
            return
        
        # 调试信息：检查输入数据
        # logger.info(f"result_df 形状: {result_df.shape}")
        # logger.info(f"result_df 列: {result_df.columns.tolist()}")
        
        # 检查前复权价格列
        qfq_columns = ['close_qfq', 'open_qfq', 'high_qfq', 'low_qfq']
        
        # 只选择需要更新的列（主键 + 前复权价格列），避免覆盖原始价格数据
        update_columns = ['ts_code', 'trade_date', 'close_qfq', 'open_qfq', 'high_qfq', 'low_qfq']
        # 只保留存在的列
        available_columns = [col for col in update_columns if col in result_df.columns]
        
        
        if not available_columns:
            logger.warning("没有可用的更新列，无法写入数据库")
            return
        
        df_to_write = result_df[available_columns].copy()
        
        # 检查是否有前复权价格数据
        has_qfq_data = any(col in df_to_write.columns and df_to_write[col].notna().any() 
                           for col in qfq_columns)
        
        if not has_qfq_data:
            logger.warning("前复权价格列全部为空，没有数据可写入")
            logger.debug(f"前5行数据:\n{df_to_write.head()}")
            return

        # 确保日期格式正确（转换为字符串 YYYY-MM-DD）
        if 'trade_date' in df_to_write.columns:
            df_to_write['trade_date'] = df_to_write['trade_date'].apply(
                lambda x: DateHelper.parse_to_str(x) if pd.notna(x) else None
            )
        
        logger.debug(f"准备写入 {len(df_to_write)} 条记录到数据库")
        logger.debug(f"写入数据的列: {df_to_write.columns.tolist()}")
        
        try:
            if not df_to_write.empty:
                success = self.daily_storage.write(df_to_write, show_progress=False)
                if success:
                    logger.info(f"✓ 前复权数据写入数据库成功，共 {len(df_to_write)} 条记录")
                else:
                    logger.error("✗ 前复权数据写入数据库失败")
            else:
                logger.warning("df_to_write 为空，没有数据可写入")
        except Exception as e:
            logger.error(f"写入数据库时发生异常: {e}", exc_info=True)