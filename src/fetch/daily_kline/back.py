"""
前复权计算器

用于读取数据库中的daily_kline数据和复权因子数据，计算前复权价格并更新到数据库。
支持单只股票和全市场批量计算，支持全量和增量更新模式。
"""
import pandas as pd
import numpy as np
from typing import Optional, List
from loguru import logger
from decimal import Decimal

from src.storage.daily_kline_storage_mysql import DailyKlineStorageMySQL
from src.storage.adj_factor_storage_mysql import AdjFactorStorageMySQL
from core.models.orm import DailyKlineORM
from utils.date_helper import DateHelper


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
    
    def calculate_qfq(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        incremental: bool = False
    ):
        """
        计算前复权价格并更新到数据库
        
        Args:
            ts_code: 股票代码，如果为None则计算全市场所有股票
            start_date: 开始日期（YYYY-MM-DD 或 YYYYMMDD），如果为None则从最早数据开始
            end_date: 结束日期（YYYY-MM-DD 或 YYYYMMDD），如果为None则到最新数据
            incremental: 是否增量更新模式
                - False: 全量模式，重新计算所有数据并覆盖
                - True: 增量模式，只计算和更新缺失的前复权数据
        """
        if ts_code:
            # 单只股票计算
            logger.info(f"开始计算股票 {ts_code} 的前复权数据（{'增量' if incremental else '全量'}模式）")
            self._calculate_single_stock(ts_code, start_date, end_date, incremental)
        else:
            # 全市场批量计算
            logger.info(f"开始计算全市场股票的前复权数据（{'增量' if incremental else '全量'}模式）")
            self._calculate_all_stocks(start_date, end_date, incremental)
    
    def _calculate_all_stocks(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        incremental: bool = False
    ):
        """
        计算全市场所有股票的前复权数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            incremental: 是否增量更新模式
        """
        # 获取所有股票代码
        ts_codes = self._get_all_stock_codes()
        if not ts_codes:
            logger.warning("未找到任何股票代码，无法进行计算")
            return
        
        logger.info(f"找到 {len(ts_codes)} 只股票，开始批量计算前复权数据")
        
        # 按股票分组处理
        success_count = 0
        error_count = 0
        
        for idx, ts_code in enumerate(ts_codes, 1):
            try:
                logger.debug(f"处理股票 {ts_code} ({idx}/{len(ts_codes)})")
                self._calculate_single_stock(ts_code, start_date, end_date, incremental)
                success_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"计算股票 {ts_code} 的前复权数据失败: {e}")
        
        logger.info(f"全市场前复权计算完成：成功 {success_count} 只，失败 {error_count} 只")
    
    def _get_all_stock_codes(self) -> List[str]:
        """
        获取数据库中所有股票代码
        
        Returns:
            股票代码列表
        """
        try:
            with self.daily_storage._get_session() as session:
                # 查询所有不重复的股票代码
                query = session.query(DailyKlineORM.ts_code).distinct()
                results = query.all()
                ts_codes = [row[0] for row in results]
                return ts_codes
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []
    
    def _calculate_single_stock(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        incremental: bool = False
    ):
        """
        计算单只股票的前复权数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            incremental: 是否增量更新模式
        """
        # 1. 读取日线数据
        df_kline = self._load_daily_kline(ts_code, start_date, end_date, incremental)
        if df_kline.empty:
            logger.debug(f"股票 {ts_code} 没有日线数据，跳过")
            return
        
        # 2. 读取复权因子数据
        adj_factors_df = self._load_adj_factors(ts_code, start_date, end_date)
        if adj_factors_df is None or adj_factors_df.empty:
            raise ValueError(f"股票 {ts_code} 没有复权因子数据，无法计算前复权价格")
        
        # 3. 构建复权因子时间序列
        adj_factor_series = self._build_adj_factor_series(adj_factors_df, df_kline['trade_date'])
        
        # 4. 计算前复权价格
        df_result = self._calculate_qfq_prices(df_kline, adj_factor_series, adj_factors_df)
        
        # 5. 更新到数据库
        if not df_result.empty:
            self._update_database(df_result, incremental)
            logger.info(f"✓ 股票 {ts_code} 前复权数据更新完成，共 {len(df_result)} 条记录")
        else:
            logger.debug(f"股票 {ts_code} 没有需要更新的数据")
    
    def _load_daily_kline(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        incremental: bool = False
    ) -> pd.DataFrame:
        """
        加载日线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            incremental: 是否增量模式（只加载缺失前复权数据的记录）
        
        Returns:
            日线数据DataFrame
        """
        # 标准化日期格式
        if start_date:
            start_date = DateHelper.normalize_to_yyyy_mm_dd(start_date)
        if end_date:
            end_date = DateHelper.normalize_to_yyyy_mm_dd(end_date)
        
        # 加载数据
        df = self.daily_storage.load(ts_code, start_date or "1900-01-01", end_date or "2099-12-31")
        
        if df.empty:
            return df
        
        # 增量模式：只处理缺失前复权数据的记录
        if incremental:
            # 检查是否有缺失的前复权数据
            missing_mask = (
                df['close_qfq'].isna() |
                df['open_qfq'].isna() |
                df['high_qfq'].isna() |
                df['low_qfq'].isna()
            )
            df = df[missing_mask].copy()
            if not df.empty:
                logger.debug(f"股票 {ts_code} 增量模式：找到 {len(df)} 条缺失前复权数据的记录")
        
        return df
    
    def _load_adj_factors(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        加载复权因子数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            复权因子DataFrame，如果无数据则返回None
        """
        # 标准化日期格式
        if start_date:
            start_date = DateHelper.normalize_to_yyyy_mm_dd(start_date)
        if end_date:
            end_date = DateHelper.normalize_to_yyyy_mm_dd(end_date)
        
        # 加载复权因子数据（不限制日期范围，需要获取所有历史复权因子）
        adj_factors_df = self.adj_storage.load(ts_code=ts_code)
        
        if adj_factors_df is None or adj_factors_df.empty:
            return None
        
        # 确保trade_date是日期类型
        if 'trade_date' in adj_factors_df.columns:
            adj_factors_df['trade_date'] = pd.to_datetime(adj_factors_df['trade_date'], errors='coerce')
        
        # 按日期排序
        adj_factors_df = adj_factors_df.sort_values('trade_date').reset_index(drop=True)
        
        return adj_factors_df
    
    def _build_adj_factor_series(
        self,
        adj_factors_df: pd.DataFrame,
        trade_dates: pd.Series
    ) -> pd.Series:
        """
        构建复权因子时间序列
        
        对于每个交易日，找到 ≤ 该日期的最近复权因子。
        如果没有复权因子，raise错误。
        
        Args:
            adj_factors_df: 复权因子DataFrame，包含trade_date和adj_factor列
            trade_dates: 交易日序列（可能是Series或列）
        
        Returns:
            每个交易日对应的复权因子Series（索引为trade_date）
        
        Raises:
            ValueError: 如果股票没有复权因子数据
        """
        if adj_factors_df.empty:
            raise ValueError("复权因子数据为空，无法构建时间序列")
        
        # 确保trade_date是日期类型
        if not pd.api.types.is_datetime64_any_dtype(adj_factors_df['trade_date']):
            adj_factors_df['trade_date'] = pd.to_datetime(adj_factors_df['trade_date'], errors='coerce')
        
        # 处理trade_dates（可能是Series或列）
        if isinstance(trade_dates, pd.Series):
            trade_dates_list = trade_dates.tolist()
        else:
            trade_dates_list = trade_dates.tolist() if hasattr(trade_dates, 'tolist') else list(trade_dates)
        
        # 转换为datetime
        trade_dates_list = [pd.to_datetime(d, errors='coerce') for d in trade_dates_list]
        
        # 创建复权因子字典（日期 -> 因子）
        adj_factor_dict = dict(zip(adj_factors_df['trade_date'], adj_factors_df['adj_factor']))
        
        # 获取所有复权因子日期（排序）
        adj_dates = sorted(adj_factors_df['trade_date'].dropna().tolist())
        
        if not adj_dates:
            raise ValueError("复权因子数据中没有有效的日期")
        
        # 为每个交易日找到对应的复权因子
        result_dict = {}
        
        for trade_date in trade_dates_list:
            if pd.isna(trade_date):
                continue
            
            # 找到 ≤ trade_date 的最近复权因子日期
            valid_adj_dates = [d for d in adj_dates if d <= trade_date]
            
            if not valid_adj_dates:
                # 如果该交易日之前没有任何复权因子，raise错误
                raise ValueError(
                    f"交易日 {trade_date.strftime('%Y-%m-%d')} 之前没有复权因子数据，"
                    f"无法计算前复权价格。最早的复权因子日期为 {adj_dates[0].strftime('%Y-%m-%d')}"
                )
            
            # 使用最近的复权因子
            latest_adj_date = max(valid_adj_dates)
            result_dict[trade_date] = adj_factor_dict[latest_adj_date]
        
        # 创建Series
        result_series = pd.Series(result_dict)
        
        return result_series
    
    def _calculate_qfq_prices(
        self,
        df_kline: pd.DataFrame,
        adj_factor_series: pd.Series,
        adj_factors_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        计算前复权价格
        
        公式：前复权价(T) = 未复权价(T) × 最新复权因子 / 历史复权因子(T)
        
        Args:
            df_kline: 日线数据DataFrame
            adj_factor_series: 每个交易日对应的历史复权因子Series
            adj_factors_df: 复权因子DataFrame（用于获取最新复权因子）
        
        Returns:
            包含前复权价格的计算结果DataFrame
        """
        df_result = df_kline.copy()
        
        # 确保trade_date是日期类型
        if not pd.api.types.is_datetime64_any_dtype(df_result['trade_date']):
            df_result['trade_date'] = pd.to_datetime(df_result['trade_date'], errors='coerce')
        
        # 获取最新复权因子（所有复权因子中日期最大的那个）
        if adj_factors_df is not None and not adj_factors_df.empty:
            # 从原始复权因子数据中获取最新日期对应的因子
            latest_adj_factor = adj_factors_df.loc[adj_factors_df['trade_date'].idxmax(), 'adj_factor']
        else:
            # 如果没有提供原始数据，从Series中获取（Series的索引是日期，值是因子）
            # 找到日期最大的索引对应的值
            if len(adj_factor_series) > 0:
                latest_date = adj_factor_series.index.max()
                latest_adj_factor = adj_factor_series[latest_date]
            else:
                raise ValueError("无法获取最新复权因子")
        
        if pd.isna(latest_adj_factor) or latest_adj_factor <= 0:
            raise ValueError(f"最新复权因子无效: {latest_adj_factor}")
        
        # 为每个交易日计算前复权价格
        for idx, row in df_result.iterrows():
            trade_date = row['trade_date']
            
            # 从Series中获取历史复权因子（使用日期匹配）
            if trade_date in adj_factor_series.index:
                hist_adj_factor = adj_factor_series[trade_date]
            else:
                # 尝试使用最接近的日期
                if len(adj_factor_series) > 0:
                    # 找到最接近的日期
                    closest_date = min(adj_factor_series.index, key=lambda x: abs((x - trade_date).days))
                    hist_adj_factor = adj_factor_series[closest_date]
                else:
                    logger.warning(f"交易日 {trade_date} 无法找到对应的复权因子，跳过该记录")
                    continue
            
            if pd.isna(hist_adj_factor) or hist_adj_factor <= 0:
                logger.warning(f"交易日 {trade_date} 的历史复权因子无效: {hist_adj_factor}，跳过该记录")
                continue
            
            # 计算前复权价格
            ratio = latest_adj_factor / hist_adj_factor
            
            # 计算各个价格字段的前复权价格
            if pd.notna(row['close']) and row['close'] > 0:
                df_result.at[idx, 'close_qfq'] = round(float(row['close']) * ratio, 2)
            
            if pd.notna(row['open']) and row['open'] > 0:
                df_result.at[idx, 'open_qfq'] = round(float(row['open']) * ratio, 2)
            
            if pd.notna(row['high']) and row['high'] > 0:
                df_result.at[idx, 'high_qfq'] = round(float(row['high']) * ratio, 2)
            
            if pd.notna(row['low']) and row['low'] > 0:
                df_result.at[idx, 'low_qfq'] = round(float(row['low']) * ratio, 2)
        
        # 只返回需要更新的列（主键 + 前复权字段）
        update_columns = ['ts_code', 'trade_date', 'close_qfq', 'open_qfq', 'high_qfq', 'low_qfq']
        df_result = df_result[update_columns].copy()
        
        # 确保日期格式正确（转换为字符串 YYYY-MM-DD）
        df_result['trade_date'] = df_result['trade_date'].apply(
            lambda x: DateHelper.parse_to_str(x) if pd.notna(x) else None
        )
        
        return df_result
    
    def _update_database(self, df: pd.DataFrame, incremental: bool = False):
        """
        更新数据库
        
        Args:
            df: 包含前复权价格的数据DataFrame
            incremental: 是否增量模式
        """
        if df.empty:
            return
        
        # 使用存储管理器的write方法批量更新
        # write方法内部使用UPSERT，会自动处理重复数据
        self.daily_storage.write(df, show_progress=True)

