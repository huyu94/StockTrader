"""
日线行情存储管理器（MySQL版本）
使用MySQL数据库和SQLAlchemy ORM，支持批量写入
"""
import pandas as pd
from typing import Optional, List, Union
from loguru import logger
from .mysql_base import MySQLBaseStorage
from .orm_models import Base, DailyKlineORM
from utils.date_helper import DateHelper
import dotenv

dotenv.load_dotenv()


class DailyKlineStorageMySQL(MySQLBaseStorage):
    """日线行情存储管理器（MySQL版本）
    使用MySQL数据库，支持更好的并发和性能
    
    优势：
    1. 支持更高的并发连接
    2. 更好的事务处理
    3. 支持连接池管理
    4. 支持UPSERT（INSERT ... ON DUPLICATE KEY UPDATE），自动去重
    """
    
    def __init__(self):
        super().__init__()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建所有表（如果不存在）
            Base.metadata.create_all(bind=self._get_engine())
            logger.debug("Daily kline table initialized in MySQL")
        except Exception as e:
            logger.error(f"Failed to initialize daily kline table: {e}")
            raise
    
    
    

    
    def write(self, df: pd.DataFrame, show_progress: bool = True, incremental: bool = True) -> bool:
        """
        写入股票数据（统一接口，支持批量写入）
        
        注意：
        - 假设传入的 DataFrame 已经是正确格式，不做额外验证
        - 数据验证应该在外层使用 Pydantic 模型完成
        - DataFrame 必须包含 ts_code 列
        - 支持单只股票或多只股票的数据写入
        
        :param df: 股票数据 DataFrame，必须包含 ts_code 和 trade_date 列
        :param show_progress: 是否显示进度条
        :param incremental: 是否增量更新（默认True）
            - True: 增量更新，跳过已存在的主键记录（使用 INSERT IGNORE）
            - False: 全量更新，覆盖已存在的主键记录（使用 UPSERT）
        :return: True 表示成功，False 表示失败
        """
        if df.empty:
            logger.info("DataFrame 为空，无需写入")
            return True
        
        try:
            update_mode = "增量更新" if incremental else "全量更新"
            logger.info(f"开始写入日线数据（{update_mode}），共 {len(df)} 条记录...")
            
            # 确保日期格式正确（YYYY-MM-DD用于MySQL存储）
            df_copy = df.copy()

            
            # 获取表中实际存在的列
            model_columns = {col.name for col in DailyKlineORM.__table__.columns}
            available_columns = [col for col in df_copy.columns if col in model_columns]
            
            if not available_columns:
                logger.error("No matching columns found between DataFrame and table")
                return False
            
            df_to_write = df_copy[available_columns].copy()
            
            # 根据增量模式选择写入方式
            with self._get_session() as session:
                if incremental:
                    # 增量更新：使用 INSERT IGNORE，跳过已存在的记录
                    inserted_count = self._bulk_insert_dataframe(
                        session, DailyKlineORM, df_to_write, 
                        ignore_duplicates=True, show_progress=show_progress
                    )
                else:
                    # 全量更新：使用 UPSERT，覆盖已存在的记录
                    inserted_count = self._bulk_upsert_dataframe(
                        session, DailyKlineORM, df_to_write, show_progress=show_progress
                    )
            
            logger.info(f"✓ 日线数据写入成功（{update_mode}），共写入 {inserted_count} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to write data: {e}")
            return False

    def load(
    self, 
    start_date: Optional[str],
    end_date: Optional[str],
    ts_codes: Union[str, List[str]] = None 
    ) -> pd.DataFrame:
        """
        读取日线数据
        
        支持两种使用方式：
        1. 单只股票 + 日期范围：load("000001.SZ", "2024-01-01", "2024-12-31")
        2. 多只股票（可选日期范围）：load(["000001.SZ", "000002.SZ"], "2024-01-01", "2024-12-31")
        3. 多只股票（无日期范围）：load(["000001.SZ", "000002.SZ"])
        
        Args:
            ts_codes: 股票代码，可以是单个字符串或字符串列表
            start_date: 开始日期（可选），支持 YYYYMMDD 或 YYYY-MM-DD 格式
            end_date: 结束日期（可选），支持 YYYYMMDD 或 YYYY-MM-DD 格式
            
        Returns:
            包含日线数据的 DataFrame，如果无数据或出错则返回空 DataFrame
        """
        try:
            # 参数验证
            if not ts_codes:
                logger.warning("ts_codes is empty")
                return pd.DataFrame()
            
            # 统一转换为列表格式
            if isinstance(ts_codes, str):
                ts_codes_list = [ts_codes]
            else:
                ts_codes_list = ts_codes
            
            if not ts_codes_list:
                return pd.DataFrame()
            
            # 处理日期范围（如果提供）
            start_date_normalized = None
            end_date_normalized = None
            
            if start_date and end_date:
                try:
                    start_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(start_date)
                    end_date_normalized = DateHelper.normalize_to_yyyy_mm_dd(end_date)
                except Exception as e:
                    logger.error(f"Invalid date format: start={start_date}, end={end_date}, error={e}")
                    return pd.DataFrame()
            
            with self._get_session() as session:
                query = session.query(DailyKlineORM).filter(
                    DailyKlineORM.ts_code.in_(ts_codes_list)
                )
                
                # 如果提供了日期范围，添加日期过滤条件
                if start_date_normalized and end_date_normalized:
                    query = query.filter(
                        DailyKlineORM.trade_date >= start_date_normalized,
                        DailyKlineORM.trade_date <= end_date_normalized
                    )
                
                # 排序
                if len(ts_codes_list) == 1:
                    # 单只股票按日期排序
                    query = query.order_by(DailyKlineORM.trade_date)
                else:
                    # 多只股票按股票代码和日期排序
                    query = query.order_by(DailyKlineORM.ts_code, DailyKlineORM.trade_date)
                
                results = query.all()
                
                if not results:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                data = [DailyKlineORM._model_to_dict(row) for row in results]
                df = pd.DataFrame(data)
                
                # 转换trade_date为datetime
                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors='coerce')
                
                return df
        except Exception as e:
            logger.error(f"Failed to load daily kline: {e}")
            return pd.DataFrame()
    
    def get_stock_count(self) -> int:
        """获取数据库中股票数量"""
        try:
            with self._get_session() as session:
                # 使用distinct查询
                count = session.query(DailyKlineORM.ts_code).distinct().count()
                return count
        except Exception as e:
            logger.error(f"Failed to get stock count: {e}")
            return 0
    
    def get_total_rows(self) -> int:
        """获取数据库总行数"""
        try:
            with self._get_session() as session:
                count = session.query(DailyKlineORM).count()
                return count
        except Exception as e:
            logger.error(f"Failed to get total rows: {e}")
            return 0


    def get_all_ts_codes(self) -> List[str]:
        """获取数据库中所有股票代码"""
        try:
            with self._get_session() as session:
                query = session.query(DailyKlineORM.ts_code).distinct()
                results = query.all()
                ts_codes = [row[0] for row in results]
                return ts_codes
        except Exception as e:
            logger.error(f"Failed to get all ts codes: {e}")
            return []

