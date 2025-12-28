"""
MySQL存储基类
提供统一的数据库连接和基础功能，使用SQLAlchemy ORM
"""
import os
import pandas as pd
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
from loguru import logger
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import dotenv

dotenv.load_dotenv()


class MySQLBaseStorage:
    """MySQL存储基类"""
    
    _engine = None
    _SessionLocal = None
    
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
                pool_pre_ping=True,  # 连接前检查连接是否有效
                echo=False,  # 设置为True可以打印SQL语句（调试用）
            )
            logger.info(f"MySQL engine created: {host}:{port}/{database}")
        
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
    
    def __init__(self):
        """初始化MySQL存储基类"""
        # 确保引擎和Session工厂已创建
        self._get_engine()
        self._get_session_factory()
        
        # 初始化数据库表结构
        self._init_database()
    
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
    
    def _init_database(self):
        """初始化数据库表结构（子类实现）"""
        raise NotImplementedError("Subclass must implement _init_database")
    
    def _bulk_upsert_dataframe(
        self,
        session: Session,
        model_class,
        df: pd.DataFrame,
        chunk_size: int = 1000,
        preserve_null_columns: list = None
    ) -> int:
        """
        批量写入DataFrame到数据库（支持UPSERT）
        
        Args:
            session: SQLAlchemy会话
            model_class: ORM模型类
            df: 要写入的DataFrame
            chunk_size: 每批写入的行数
            preserve_null_columns: 如果新值为NULL则保留现有值的列名列表（使用COALESCE）
            
        Returns:
            写入的行数
        """
        if df is None or df.empty:
            return 0
        
        # 获取表名和主键
        table = model_class.__table__
        table_name = table.name
        primary_keys = [key.name for key in table.primary_key.columns]
        
        # 将DataFrame转换为字典列表
        # 处理NaN值，转换为None
        df_clean = df.copy()
        df_clean = df_clean.where(pd.notna(df_clean), None)
        
        records = df_clean.to_dict('records')
        total_rows = len(records)
        
        # 处理 preserve_null_columns
        preserve_null_set = set(preserve_null_columns) if preserve_null_columns else set()
        
        # 计算总批次数
        total_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        # 分批处理，使用 tqdm 显示进度
        inserted_count = 0
        with tqdm(total=total_chunks, desc=f"写入 {table_name}", unit="批", leave=False) as pbar:
            for i in range(0, total_rows, chunk_size):
                chunk = records[i:i + chunk_size]
                chunk_num = i // chunk_size + 1
                
                # 更新进度条描述
                pbar.set_description(f"写入 {table_name} ({chunk_num}/{total_chunks} 批, {len(chunk)} 条)")
                
                # 构建UPSERT语句（MySQL的INSERT ... ON DUPLICATE KEY UPDATE）
                # 获取所有列名
                columns = list(chunk[0].keys())
                
                # 构建INSERT部分
                columns_str = ', '.join([f'`{col}`' for col in columns])
                placeholders = ', '.join([f':{col}' for col in columns])
                
                # 构建UPDATE部分（更新所有非主键列）
                update_columns = [col for col in columns if col not in primary_keys]
                if update_columns:
                    # 对于需要保留NULL的列，使用COALESCE；否则直接使用VALUES
                    update_parts = []
                    for col in update_columns:
                        if col in preserve_null_set:
                            # 如果新值为NULL，保留现有值；否则使用新值
                            update_parts.append(f'`{col}`=COALESCE(VALUES(`{col}`), `{col}`)')
                        else:
                            # 直接使用新值（即使为NULL也会覆盖）
                            update_parts.append(f'`{col}`=VALUES(`{col}`)')
                    
                    update_clause = ', '.join(update_parts)
                    
                    # 完整的UPSERT SQL
                    sql = f"""
                        INSERT INTO `{table_name}` ({columns_str})
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """
                else:
                    # 如果没有非主键列需要更新，使用简单的INSERT IGNORE
                    sql = f"""
                        INSERT IGNORE INTO `{table_name}` ({columns_str})
                        VALUES ({placeholders})
                    """
                
                # 执行批量插入
                # 构建参数绑定
                stmt = text(sql)
                # 为每条记录执行插入
                for record in chunk:
                    params = {col: record.get(col) for col in columns}
                    session.execute(stmt, params)
                inserted_count += len(chunk)
                
                # 更新进度条
                pbar.update(1)
        
        logger.debug(f"Bulk upserted {inserted_count} rows into {table_name}")
        return inserted_count
    
    def _bulk_insert_dataframe(
    self,
    session: Session,
    model_class,
    df: pd.DataFrame,
    chunk_size: int = 1000,
    ignore_duplicates: bool = False
    ) -> int:
        """
        批量插入DataFrame到数据库（不处理重复，直接插入）
        
        Args:
            session: SQLAlchemy会话
            model_class: ORM模型类
            df: 要写入的DataFrame
            chunk_size: 每批写入的行数
            ignore_duplicates: 如果为True，遇到主键冲突时跳过（使用INSERT IGNORE）
                
        Returns:
            插入的行数
        """
        if df is None or df.empty:
            return 0
        
        # 将DataFrame转换为字典列表
        df_clean = df.copy()
        df_clean = df_clean.where(pd.notna(df_clean), None)
        
        records = df_clean.to_dict('records')
        total_rows = len(records)
        
        # 获取表名
        table = model_class.__table__
        table_name = table.name
        
        # 计算总批次数
        total_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        inserted_count = 0
        
        if ignore_duplicates:
            # 使用 INSERT IGNORE 跳过重复数据
            with tqdm(total=total_chunks, desc=f"插入 {table_name}", unit="批", leave=False) as pbar:
                for i in range(0, total_rows, chunk_size):
                    chunk = records[i:i + chunk_size]
                    chunk_num = i // chunk_size + 1
                    
                    pbar.set_description(f"插入 {table_name} ({chunk_num}/{total_chunks} 批, {len(chunk)} 条)")
                    
                    # 构建 INSERT IGNORE 语句
                    columns = list(chunk[0].keys())
                    columns_str = ', '.join([f'`{col}`' for col in columns])
                    placeholders = ', '.join([f':{col}' for col in columns])
                    
                    sql = f"""
                        INSERT IGNORE INTO `{table_name}` ({columns_str})
                        VALUES ({placeholders})
                    """
                    
                    stmt = text(sql)
                    for record in chunk:
                        params = {col: record.get(col) for col in columns}
                        session.execute(stmt, params)
                    
                    inserted_count += len(chunk)
                    pbar.update(1)
        else:
            # 使用 SQLAlchemy 的 bulk_insert_mappings（遇到重复会报错）
            with tqdm(total=total_chunks, desc=f"插入 {table_name}", unit="批", leave=False) as pbar:
                for i in range(0, total_rows, chunk_size):
                    chunk = records[i:i + chunk_size]
                    chunk_num = i // chunk_size + 1
                    
                    pbar.set_description(f"插入 {table_name} ({chunk_num}/{total_chunks} 批, {len(chunk)} 条)")
                    
                    session.bulk_insert_mappings(model_class, chunk)
                    inserted_count += len(chunk)
                    pbar.update(1)
        
        logger.debug(f"Bulk inserted {inserted_count} rows into {table_name}")
        return inserted_count
