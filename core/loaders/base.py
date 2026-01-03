"""
加载器基类模块

定义所有数据加载器的抽象基类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import os
import pandas as pd
import numpy as np
from loguru import logger
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import dotenv

from core.common.exceptions import LoaderException

dotenv.load_dotenv()


class BaseLoader(ABC):
    """
    加载器抽象基类
    
    所有数据加载器都应该继承此类并实现 load 方法。
    基类提供加载策略（append/replace/upsert）、批量处理等通用功能框架。
    """
    
    # 加载策略常量
    LOAD_STRATEGY_APPEND = "append"  # 追加数据
    LOAD_STRATEGY_REPLACE = "replace"  # 替换数据
    LOAD_STRATEGY_UPSERT = "upsert"  # 存在则更新，不存在则插入
    
    # 数据库连接（类级别单例）
    _engine = None
    _SessionLocal = None
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化加载器
        
        Args:
            config: 加载器配置字典，包含表名、批量大小等配置
        """
        self.config = config or {}
        self.table = self.config.get("table", "")
        self.batch_size = self.config.get("batch_size", 1000)
        self.upsert_keys = self.config.get("upsert_keys", [])
        logger.debug(f"初始化加载器: {self.__class__.__name__}, 表: {self.table}")
    
    @classmethod
    def _get_engine(cls):
        """获取数据库引擎（单例模式）"""
        if cls._engine is None:
            # 从环境变量读取MySQL配置
            host = os.getenv("MYSQL_HOST", "localhost")
            port = int(os.getenv("MYSQL_PORT", "3306"))
            user = os.getenv("MYSQL_USER", "root")
            password = os.getenv("MYSQL_PASSWORD", "")
            database = os.getenv("MYSQL_DATABASE", "stock_data")
            charset = os.getenv("MYSQL_CHARSET", "utf8mb4")
            
            # 构建连接URL
            connection_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={charset}"
            
            # 创建引擎，配置连接池
            cls._engine = create_engine(
                connection_url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                echo=False,
            )
            logger.debug(f"MySQL engine created: {host}:{port}/{database}")
        
        return cls._engine
    
    @classmethod
    def _get_session_factory(cls):
        """获取Session工厂（单例模式）"""
        if cls._SessionLocal is None:
            cls._SessionLocal = sessionmaker(
                bind=cls._get_engine(),
                autocommit=False,
                autoflush=False
            )
        return cls._SessionLocal
    
    @contextmanager
    def _get_session(self):
        """获取数据库会话（上下文管理器，自动关闭）"""
        SessionLocal = self._get_session_factory()
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @abstractmethod
    def load(self, data: pd.DataFrame, strategy: str) -> None:
        """
        加载数据到数据库的核心方法
        
        Args:
            data: 待加载的数据 DataFrame
            strategy: 加载策略，必须是 LOAD_STRATEGY_APPEND、LOAD_STRATEGY_REPLACE 或 LOAD_STRATEGY_UPSERT 之一
            
        Raises:
            LoaderException: 当加载失败时抛出异常
        """
        pass
    
    @abstractmethod
    def _get_orm_model(self):
        """
        获取对应的ORM模型类
        
        Returns:
            ORM模型类
        """
        pass
    
    @abstractmethod
    def _get_required_columns(self) -> List[str]:
        """
        获取必需的数据列
        
        Returns:
            必需列名列表
        """
        pass
    
    def _validate_data_before_load(self, data: pd.DataFrame) -> bool:
        """
        加载前验证数据
        
        Args:
            data: 待验证的数据
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            LoaderException: 验证失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("数据为空，跳过加载")
            return False
        
        # 检查必需列
        required_columns = self._get_required_columns()
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise LoaderException(f"缺少必需的列: {missing_columns}")
        
        return True
    
    def _load_append(self, data: pd.DataFrame) -> None:
        """
        追加模式加载数据
        
        Args:
            data: 待加载的数据
        """
        if not self._validate_data_before_load(data):
            return
        
        model_class = self._get_orm_model()
        
        # 获取表中实际存在的列
        model_columns = {col.name for col in model_class.__table__.columns}
        available_columns = [col for col in data.columns if col in model_columns]
        
        if not available_columns:
            raise LoaderException("DataFrame 中没有与表匹配的列")
        
        df_to_write = data[available_columns].copy()
        df_to_write = df_to_write.where(pd.notna(df_to_write), None)
        
        # 使用 INSERT IGNORE 跳过重复数据
        with self._get_session() as session:
            inserted_count = self._bulk_insert_dataframe(
                session, model_class, df_to_write,
                ignore_duplicates=True,
                show_progress=False
            )
        
        logger.debug(f"追加模式加载完成，共插入 {inserted_count} 条记录")
    
    def _load_replace(self, data: pd.DataFrame) -> None:
        """
        替换模式加载数据
        
        Args:
            data: 待加载的数据
        """
        if not self._validate_data_before_load(data):
            return
        
        model_class = self._get_orm_model()
        table = model_class.__table__
        table_name = table.name
        
        # 获取主键列
        primary_keys = [key.name for key in table.primary_key.columns]
        
        # 获取要删除的数据的主键值
        if not primary_keys:
            raise LoaderException("表没有主键，无法使用替换模式")
        
        # 获取表中实际存在的列
        model_columns = {col.name for col in model_class.__table__.columns}
        available_columns = [col for col in data.columns if col in model_columns]
        
        if not available_columns:
            raise LoaderException("DataFrame 中没有与表匹配的列")
        
        df_to_write = data[available_columns].copy()
        df_to_write = df_to_write.where(pd.notna(df_to_write), None)
        
        # 构建删除条件：删除所有匹配主键的记录
        with self._get_session() as session:
            # 删除匹配的记录
            if len(primary_keys) == 1:
                # 单主键：构建 IN 子句
                key_values = df_to_write[primary_keys[0]].unique().tolist()
                if key_values:
                    # 构建占位符
                    placeholders = ', '.join([f':val_{i}' for i in range(len(key_values))])
                    delete_stmt = text(f"DELETE FROM `{table_name}` WHERE `{primary_keys[0]}` IN ({placeholders})")
                    params = {f'val_{i}': val for i, val in enumerate(key_values)}
                    session.execute(delete_stmt, params)
            else:
                # 复合主键：需要逐条删除
                for _, row in df_to_write.iterrows():
                    conditions = []
                    params = {}
                    for i, key in enumerate(primary_keys):
                        conditions.append(f"`{key}` = :key_{i}")
                        params[f"key_{i}"] = row[key]
                    delete_stmt = text(f"DELETE FROM `{table_name}` WHERE {' AND '.join(conditions)}")
                    session.execute(delete_stmt, params)
            
            # 插入新数据
            inserted_count = self._bulk_insert_dataframe(
                session, model_class, df_to_write,
                ignore_duplicates=False,
                show_progress=False
            )
        
        logger.debug(f"替换模式加载完成，共插入 {inserted_count} 条记录")
    
    def _load_upsert(self, data: pd.DataFrame) -> None:
        """
        更新或插入模式加载数据
        
        Args:
            data: 待加载的数据
        """
        if not self._validate_data_before_load(data):
            return
        
        model_class = self._get_orm_model()
        
        # 获取表中实际存在的列
        model_columns = {col.name for col in model_class.__table__.columns}
        available_columns = [col for col in data.columns if col in model_columns]
        
        if not available_columns:
            raise LoaderException("DataFrame 中没有与表匹配的列")
        
        df_to_write = data[available_columns].copy()
        df_to_write = df_to_write.where(pd.notna(df_to_write), None)
        
        # 获取需要保留NULL的列（从配置中读取）
        preserve_null_columns = self.config.get("preserve_null_columns", [])
        
        # 使用 UPSERT
        with self._get_session() as session:
            inserted_count = self._bulk_upsert_dataframe(
                session, model_class, df_to_write,
                preserve_null_columns=preserve_null_columns,
                show_progress=False
            )
        
        logger.debug(f"更新或插入模式加载完成，共处理 {inserted_count} 条记录")
    
    def _bulk_insert_dataframe(
        self,
        session: Session,
        model_class,
        df: pd.DataFrame,
        ignore_duplicates: bool = False,
        show_progress: bool = False
    ) -> int:
        """
        批量插入DataFrame到数据库
        
        Args:
            session: SQLAlchemy会话
            model_class: ORM模型类
            df: 要写入的DataFrame
            ignore_duplicates: 如果为True，遇到主键冲突时跳过
            show_progress: 是否显示进度条
            
        Returns:
            插入的行数
        """
        if df is None or df.empty:
            return 0
        
        table = model_class.__table__
        table_name = table.name
        
        records = df.to_dict('records')
        total_rows = len(records)
        
        if total_rows == 0:
            return 0
        
        # 将字典中的 NaN 值转换为 None，确保 MySQL 兼容性
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
                    record[key] = None
        
        columns = list(records[0].keys())
        columns_str = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join([f':{col}' for col in columns])
        
        if ignore_duplicates:
            sql = f"INSERT IGNORE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        else:
            sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        stmt = text(sql)
        inserted_count = 0
        
        # 分批处理
        for i in range(0, total_rows, self.batch_size):
            chunk = records[i:i + self.batch_size]
            for record in chunk:
                params = {col: record.get(col) for col in columns}
                session.execute(stmt, params)
            inserted_count += len(chunk)
        
        return inserted_count
    
    def _bulk_upsert_dataframe(
        self,
        session: Session,
        model_class,
        df: pd.DataFrame,
        preserve_null_columns: List[str] = None,
        show_progress: bool = False
    ) -> int:
        """
        批量写入DataFrame到数据库（支持UPSERT）
        
        Args:
            session: SQLAlchemy会话
            model_class: ORM模型类
            df: 要写入的DataFrame
            preserve_null_columns: 如果新值为NULL则保留现有值的列名列表
            show_progress: 是否显示进度条
            
        Returns:
            写入的行数
        """
        if df is None or df.empty:
            return 0
        
        table = model_class.__table__
        table_name = table.name
        primary_keys = [key.name for key in table.primary_key.columns]
        
        records = df.to_dict('records')
        total_rows = len(records)
        
        if total_rows == 0:
            return 0
        
        # 将字典中的 NaN 值转换为 None，确保 MySQL 兼容性
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
                    record[key] = None
        
        preserve_null_set = set(preserve_null_columns) if preserve_null_columns else set()
        
        columns = list(records[0].keys())
        columns_str = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join([f':{col}' for col in columns])
        
        # 构建UPDATE部分（更新所有非主键列）
        update_columns = [col for col in columns if col not in primary_keys]
        if update_columns:
            update_parts = []
            for col in update_columns:
                if col in preserve_null_set:
                    update_parts.append(f'`{col}`=COALESCE(VALUES(`{col}`), `{col}`)')
                else:
                    update_parts.append(f'`{col}`=VALUES(`{col}`)')
            
            update_clause = ', '.join(update_parts)
            sql = f"""
                INSERT INTO `{table_name}` ({columns_str})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
        else:
            sql = f"""
                INSERT IGNORE INTO `{table_name}` ({columns_str})
                VALUES ({placeholders})
            """
        
        stmt = text(sql)
        inserted_count = 0
        
        # 分批处理
        for i in range(0, total_rows, self.batch_size):
            chunk = records[i:i + self.batch_size]
            for record in chunk:
                params = {col: record.get(col) for col in columns}
                session.execute(stmt, params)
            inserted_count += len(chunk)
        
        return inserted_count

