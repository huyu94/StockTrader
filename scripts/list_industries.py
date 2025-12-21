"""
列出所有可用的行业

查询数据库中的所有行业列表，用于策略筛选。
"""

import sys
import os
import sqlite3
from collections import Counter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import setup_logger
from src.manager import Manager
from project_var import DATA_DIR
from loguru import logger


def list_industries():
    """列出所有行业及其股票数量"""
    setup_logger()
    
    logger.info("正在查询行业列表...")
    
    # 方法1：使用Manager
    manager = Manager()
    basic_df = manager.all_basic_info
    
    if basic_df is None or basic_df.empty:
        logger.error("无法获取股票基本信息，请先运行数据更新")
        return
    
    # 检查是否有industry列
    if 'industry' not in basic_df.columns:
        logger.warning("basic_info表中没有industry列")
        return
    
    # 统计行业
    industries = basic_df['industry'].fillna('未知').tolist()
    industry_counts = Counter(industries)
    
    # 按股票数量排序
    sorted_industries = sorted(industry_counts.items(), key=lambda x: x[1], reverse=True)
    
    logger.info(f"\n{'='*80}")
    logger.info(f"共找到 {len(sorted_industries)} 个行业")
    logger.info(f"{'='*80}\n")
    
    # 显示行业列表
    logger.info(f"{'行业名称':<30} {'股票数量':<10}")
    logger.info("-" * 80)
    
    for industry, count in sorted_industries:
        logger.info(f"{industry:<30} {count:<10}")
    
    logger.info(f"\n{'='*80}")
    logger.info("使用示例：")
    logger.info("  python scripts/run_strategies.py --industries \"银行,证券\"")
    logger.info("  python scripts/run_strategies.py --industries \"计算机应用,软件开发\"")
    logger.info(f"{'='*80}\n")


if __name__ == "__main__":
    list_industries()

