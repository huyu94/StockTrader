"""
初始化 MySQL 数据库
1. 检查数据库是否存在，不存在则创建
2. 检查所有表是否存在，不存在则创建
"""
import sys
import os
import pymysql
from dotenv import load_dotenv
from pathlib import Path
from loguru import logger

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

load_dotenv()

from sqlalchemy import create_engine
from core.models.orm import Base


def create_database_if_not_exists(host, port, user, password, database):
    """
    检查数据库是否存在，不存在则创建
    
    Args:
        host: MySQL 主机地址
        port: MySQL 端口
        user: MySQL 用户名
        password: MySQL 密码
        database: 数据库名
        
    Returns:
        bool: 数据库是否已存在（True）或刚创建（False）
    """
    # 连接到 MySQL（不指定数据库）
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        charset='utf8mb4'
    )
    
    try:
        with connection.cursor() as cursor:
            # 检查数据库是否存在
            cursor.execute(f"SHOW DATABASES LIKE '{database}'")
            exists = cursor.fetchone() is not None
            
            if exists:
                logger.info(f"数据库 '{database}' 已存在")
                return True
            else:
                # 创建数据库
                cursor.execute(
                    f"CREATE DATABASE `{database}` "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
                connection.commit()
                logger.success(f"数据库 '{database}' 创建成功！")
                return False
    finally:
        connection.close()


def create_tables_if_not_exists(host, port, user, password, database):
    """
    检查所有表是否存在，不存在则创建
    
    Args:
        host: MySQL 主机地址
        port: MySQL 端口
        user: MySQL 用户名
        password: MySQL 密码
        database: 数据库名
    """
    # 导入所有模型类，确保它们被注册到 Base.metadata
    from core.models.orm import (
        DailyKlineORM,
        BasicInfoORM,
        TradeCalendarORM,
        AdjFactorORM,
        IntradayKlineORM,
    )
    
    # 构建连接URL
    connection_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    
    try:
        # 创建引擎
        engine = create_engine(connection_url, echo=False)
        
        # 创建所有表（如果不存在）
        # checkfirst=True 表示如果表已存在则跳过
        logger.info("正在检查并创建数据库表...")
        Base.metadata.create_all(engine, checkfirst=True)
        
        logger.success("✅ 所有数据库表检查完成！")
        logger.info("已存在的表：")
        for table_name in Base.metadata.tables.keys():
            logger.info(f"  - {table_name}")
            
    except Exception as e:
        logger.error(f"创建数据库表失败: {e}")
        raise


def init_database():
    """初始化数据库和所有表"""
    logger.info("=" * 60)
    logger.info("开始初始化 MySQL 数据库...")
    logger.info("=" * 60)
    
    # 从环境变量读取配置
    host = os.getenv("MYSQL_HOST", "192.168.1.105")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DATABASE", "stock_test")
    
    logger.info(f"MySQL 配置: {host}:{port}")
    logger.info(f"数据库名: {database}")
    
    try:
        # 步骤 1: 检查并创建数据库
        logger.info("-" * 60)
        logger.info("步骤 1: 检查数据库...")
        logger.info("-" * 60)
        create_database_if_not_exists(host, port, user, password, database)
        
        # 步骤 2: 检查并创建所有表
        logger.info("-" * 60)
        logger.info("步骤 2: 检查并创建数据库表...")
        logger.info("-" * 60)
        create_tables_if_not_exists(host, port, user, password, database)
        
        logger.info("=" * 60)
        logger.success("🎉 数据库初始化完成！")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"初始化数据库失败: {e}")
        raise


if __name__ == "__main__":
    init_database()
