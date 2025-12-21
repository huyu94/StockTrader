"""
SQLite存储基类
提供统一的数据库连接和基础功能
"""
import os
import sqlite3
import pandas as pd
from contextlib import contextmanager
from typing import Optional
from loguru import logger
from project_var import DATA_DIR
import dotenv

dotenv.load_dotenv()


class SQLiteBaseStorage:
    """SQLite存储基类"""
    
    def __init__(self, db_name: str):
        data_path = os.getenv("DATA_PATH", DATA_DIR)
        self.data_dir = data_path if os.path.isabs(data_path) else os.path.join(os.getcwd(), data_path)
        self.db_path = os.path.join(self.data_dir, db_name)
        
        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 初始化数据库
        self._init_database()
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接（上下文管理器，自动关闭）"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")  # 启用WAL模式，提升并发性能
        conn.execute("PRAGMA synchronous=NORMAL")  # 平衡性能和数据安全
        conn.execute("PRAGMA cache_size=-64000")  # 64MB缓存
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_database(self):
        """初始化数据库表结构（子类实现）"""
        raise NotImplementedError("Subclass must implement _init_database")
    
    @staticmethod
    def _upsert_method(table, conn, keys, data_iter):
        """自定义写入方法，使用 INSERT OR REPLACE 实现UPSERT"""
        # pandas.to_sql 的 method 参数中，table 可能是对象或字符串
        if hasattr(table, 'name'):
            table_name = table.name
        elif isinstance(table, str):
            table_name = table
        else:
            table_name = "unknown_table"
        
        # 使用引号包裹列名，避免SQL关键字冲突
        columns = ', '.join([f'"{key}"' for key in keys])
        placeholders = ', '.join(['?' for _ in keys])
        
        # 构建 INSERT OR REPLACE 语句
        sql = f'INSERT OR REPLACE INTO "{table_name}" ({columns}) VALUES ({placeholders})'
        
        # 处理数据：将NaN转换为None（SQLite的NULL）
        def clean_data(row):
            return tuple(None if pd.isna(val) else val for val in row)
        
        # 批量执行
        cleaned_data = (clean_data(row) for row in data_iter)
        conn.executemany(sql, cleaned_data)

