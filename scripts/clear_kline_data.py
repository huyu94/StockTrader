"""
清空数据库中的日线数据

用法：
    # 清空所有日线数据（需要确认）
    uv run python scripts/clear_kline_data.py
    
    # 清空所有日线数据（跳过确认）
    uv run python scripts/clear_kline_data.py --yes
    
    # 清空指定股票的日线数据
    uv run python scripts/clear_kline_data.py --ts-code 300642.SZ
    
    # 清空指定股票的日线数据（跳过确认）
    uv run python scripts/clear_kline_data.py --ts-code 300642.SZ --yes
"""

import sys
import os
import argparse
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import setup_logger
from src.data.storage.daily_kline_storage_sqlite import DailyKlineStorageSQLite


def clear_all_kline_data(confirm: bool = True):
    """
    清空所有日线数据
    
    :param confirm: 是否需要确认
    """
    setup_logger(level_console="INFO")
    
    storage = DailyKlineStorageSQLite()
    
    # 获取清空前的统计信息
    before_count = storage.get_total_rows()
    before_stocks = storage.get_stock_count()
    
    logger.info("=" * 80)
    logger.info("准备清空所有日线数据")
    logger.info("=" * 80)
    logger.info(f"当前数据统计:")
    logger.info(f"  总行数: {before_count:,}")
    logger.info(f"  股票数量: {before_stocks:,}")
    logger.info("")
    
    if before_count == 0:
        logger.info("数据库中没有日线数据，无需清空")
        return
    
    # 确认操作
    if confirm:
        logger.warning("⚠️  警告: 此操作将删除所有日线数据，且无法恢复！")
        response = input("确认要清空所有日线数据吗？(yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            logger.info("操作已取消")
            return
    
    # 执行清空操作
    try:
        with storage._get_connection() as conn:
            # 先获取要删除的行数
            cursor = conn.execute("SELECT COUNT(*) FROM daily_kline")
            deleted_rows = cursor.fetchone()[0]
            
            # 执行删除
            conn.execute("DELETE FROM daily_kline")
            conn.commit()
        
        logger.info(f"✓ 成功清空 {deleted_rows:,} 条日线数据")
        
        # 验证清空结果
        after_count = storage.get_total_rows()
        after_stocks = storage.get_stock_count()
        
        logger.info("")
        logger.info("清空后的数据统计:")
        logger.info(f"  总行数: {after_count:,}")
        logger.info(f"  股票数量: {after_stocks:,}")
        logger.info("")
        
        if after_count == 0:
            logger.info("✓ 数据库已清空，可以重新下载数据了")
        else:
            logger.warning(f"⚠️  警告: 清空后仍有 {after_count:,} 条数据，可能存在问题")
        
    except Exception as e:
        logger.error(f"❌ 清空数据失败: {e}")
        raise


def clear_stock_kline_data(ts_code: str, confirm: bool = True):
    """
    清空指定股票的日线数据
    
    :param ts_code: 股票代码，例如 "300642.SZ"
    :param confirm: 是否需要确认
    """
    setup_logger(level_console="INFO")
    
    storage = DailyKlineStorageSQLite()
    
    # 检查股票是否存在数据
    df = storage.load(ts_code)
    if df is None or df.empty:
        logger.info(f"股票 {ts_code} 没有日线数据，无需清空")
        return
    
    before_count = len(df)
    
    logger.info("=" * 80)
    logger.info(f"准备清空股票 {ts_code} 的日线数据")
    logger.info("=" * 80)
    logger.info(f"当前数据量: {before_count:,} 条")
    logger.info("")
    
    # 确认操作
    if confirm:
        logger.warning(f"⚠️  警告: 此操作将删除股票 {ts_code} 的所有日线数据，且无法恢复！")
        response = input(f"确认要清空股票 {ts_code} 的日线数据吗？(yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            logger.info("操作已取消")
            return
    
    # 执行清空操作
    try:
        with storage._get_connection() as conn:
            # 先获取要删除的行数
            cursor = conn.execute("SELECT COUNT(*) FROM daily_kline WHERE ts_code = ?", (ts_code,))
            deleted_rows = cursor.fetchone()[0]
            
            # 执行删除
            conn.execute("DELETE FROM daily_kline WHERE ts_code = ?", (ts_code,))
            conn.commit()
        
        logger.info(f"✓ 成功清空股票 {ts_code} 的 {deleted_rows:,} 条日线数据")
        
        # 验证清空结果
        df_after = storage.load(ts_code)
        after_count = len(df_after) if df_after is not None else 0
        
        logger.info("")
        logger.info(f"清空后的数据量: {after_count:,} 条")
        
        if after_count == 0:
            logger.info(f"✓ 股票 {ts_code} 的数据已清空")
        else:
            logger.warning(f"⚠️  警告: 清空后仍有 {after_count:,} 条数据，可能存在问题")
        
    except Exception as e:
        logger.error(f"❌ 清空数据失败: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description='清空数据库中的日线数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 清空所有日线数据（需要确认）
  uv run python scripts/clear_kline_data.py
  
  # 清空所有日线数据（跳过确认）
  uv run python scripts/clear_kline_data.py --yes
  
  # 清空指定股票的日线数据
  uv run python scripts/clear_kline_data.py --ts-code 300642.SZ
  
  # 清空指定股票的日线数据（跳过确认）
  uv run python scripts/clear_kline_data.py --ts-code 300642.SZ --yes
        """
    )
    parser.add_argument(
        '--ts-code',
        type=str,
        default=None,
        help='股票代码，如果指定则只清空该股票的数据，例如 300642.SZ'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        help='跳过确认提示，直接执行清空操作'
    )
    
    args = parser.parse_args()
    
    try:
        if args.ts_code:
            # 清空指定股票的数据
            clear_stock_kline_data(args.ts_code, confirm=not args.yes)
        else:
            # 清空所有数据
            clear_all_kline_data(confirm=not args.yes)
        
        logger.info("=" * 80)
        logger.info("操作完成")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("用户中断执行")
    except Exception as e:
        logger.error(f"运行失败: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

