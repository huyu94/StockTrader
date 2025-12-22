"""
调试指定股票在指定日期是否符合少妇战法策略

用法：
    uv run python scripts/debug_stock_date.py --ts-code 300642.SZ --date 20250929
    uv run python scripts/debug_stock_date.py --ts-code 000001.SZ --date 2025-09-29
"""

import sys
import os
import argparse
from loguru import logger
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import setup_logger
from src.manager import Manager
from src.strategies import KDJStrategy


def debug_stock_date(ts_code: str, check_date: str):
    """
    调试指定股票在指定日期是否符合策略条件
    
    :param ts_code: 股票代码，例如 "300642.SZ"
    :param check_date: 检查日期，格式YYYYMMDD或YYYY-MM-DD
    """
    setup_logger(level_console="INFO")
    
    logger.info("=" * 80)
    logger.info(f"调试股票: {ts_code} 在日期: {check_date}")
    logger.info("=" * 80)
    
    # 初始化Manager和策略
    manager = Manager()
    manager.update_basic_info()
    
    strategy = KDJStrategy(storage=manager.daily_storage)
    logger.info(f"策略: {strategy.name}")
    logger.info(f"参数: J阈值={strategy.j_threshold}, 成交量周期={strategy.vol_period}, KDJ周期={strategy.kdj_period}")
    logger.info("")
    
    # 检查股票基本信息
    basic_df = manager.all_basic_info
    if basic_df is None or basic_df.empty:
        logger.error("❌ 无法获取股票基本信息")
        return
    
    stock_info = basic_df[basic_df['ts_code'] == ts_code]
    if stock_info.empty:
        logger.error(f"❌ 未找到股票代码: {ts_code}")
        logger.error(f"提示: 请检查股票代码格式，例如 300642.SZ")
        return
    
    stock_name = stock_info.iloc[0].get('name', '')
    logger.info(f"✓ 股票名称: {stock_name}")
    logger.info("")
    
    # 加载数据
    df = manager.daily_storage.load(ts_code)
    if df is None or df.empty:
        logger.error("❌ 数据库中无日线数据！")
        logger.error("请先运行: uv run scripts/run_fetch.py --mode code")
        return
    
    logger.info(f"✓ 数据库中的数据量: {len(df)} 条")
    
    # 统一日期格式
    if len(check_date) == 8 and check_date.isdigit():
        check_date_dt = pd.to_datetime(check_date, format='%Y%m%d')
        check_date_str = check_date
    else:
        check_date_dt = pd.to_datetime(check_date, errors='coerce')
        check_date_str = check_date_dt.strftime('%Y%m%d') if not pd.isna(check_date_dt) else check_date
    
    if pd.isna(check_date_dt):
        logger.error(f"❌ 无效的日期格式: {check_date}")
        return
    
    logger.info(f"✓ 检查日期: {check_date_dt.strftime('%Y-%m-%d')} ({check_date_str})")
    
    # 检查该日期是否有数据
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
        date_data = df[df['trade_date'] == check_date_dt]
        if date_data.empty:
            logger.warning(f"⚠️  警告: 该日期没有交易数据")
            # 查找最近的交易日
            before_dates = df[df['trade_date'] < check_date_dt]
            after_dates = df[df['trade_date'] > check_date_dt]
            if not before_dates.empty:
                nearest_before = before_dates.iloc[-1]['trade_date']
                logger.info(f"   最近的之前交易日: {nearest_before.strftime('%Y-%m-%d')}")
            if not after_dates.empty:
                nearest_after = after_dates.iloc[0]['trade_date']
                logger.info(f"   最近的之后交易日: {nearest_after.strftime('%Y-%m-%d')}")
            logger.info("")
        else:
            logger.info(f"✓ 该日期有交易数据")
            logger.info("")
    
    # 预处理数据（只保留检查日期及之前的数据）
    logger.info("预处理数据（计算KDJ和成交量指标）...")
    processed_df = strategy.preprocess(ts_code, check_date=check_date_str)
    
    if processed_df.empty:
        logger.error("❌ 预处理失败：数据不足或无法计算指标")
        return
    
    logger.info(f"✓ 预处理成功，数据量: {len(processed_df)} 条")
    logger.info(f"✓ 数据日期范围: {processed_df['trade_date'].min().strftime('%Y-%m-%d')} 到 {processed_df['trade_date'].max().strftime('%Y-%m-%d')}")
    logger.info("")
    
    # 获取检查日期的数据
    latest = processed_df.iloc[-1]
    latest_date = latest.get('trade_date')
    
    if pd.to_datetime(latest_date) != check_date_dt:
        print(f"⚠️  警告: 检查日期 {check_date_dt.strftime('%Y-%m-%d')} 的数据不存在")
        print(f"   使用最新可用日期: {latest_date.strftime('%Y-%m-%d')}")
        print()
    
    # 显示检查日期的详细数据
    logger.info("=" * 80)
    logger.info("检查日期的详细数据:")
    logger.info("=" * 80)
    logger.info(f"日期: {latest_date}")
    logger.info(f"收盘价: {latest.get('close', 'N/A'):.2f}")
    logger.info(f"开盘价: {latest.get('open', 'N/A'):.2f}")
    logger.info(f"最高价: {latest.get('high', 'N/A'):.2f}")
    logger.info(f"最低价: {latest.get('low', 'N/A'):.2f}")
    logger.info(f"成交量: {latest.get('vol', 'N/A'):,.0f}")
    logger.info("")
    
    # 显示检查日期的详细数据
    logger.info("=" * 80)
    logger.info("检查日期的详细数据:")
    logger.info("=" * 80)
    logger.info(f"日期: {latest_date}")
    logger.info(f"收盘价: {latest.get('close', 'N/A'):.2f}")
    logger.info(f"开盘价: {latest.get('open', 'N/A'):.2f}")
    logger.info(f"最高价: {latest.get('high', 'N/A'):.2f}")
    logger.info(f"最低价: {latest.get('low', 'N/A'):.2f}")
    logger.info(f"成交量: {latest.get('vol', 'N/A'):,.0f}")
    logger.info("")
    
    # KDJ指标
    k_value = latest.get('kdj_k', None)
    d_value = latest.get('kdj_d', None)
    j_value = latest.get('kdj_j', None)
    
    logger.info("KDJ指标:")
    if k_value is not None and not pd.isna(k_value):
        logger.info(f"  K值: {k_value:.2f}")
    else:
        logger.info(f"  K值: N/A (无效)")
    
    if d_value is not None and not pd.isna(d_value):
        logger.info(f"  D值: {d_value:.2f}")
    else:
        logger.info(f"  D值: N/A (无效)")
    
    if j_value is not None and not pd.isna(j_value):
        logger.info(f"  J值: {j_value:.2f}")
    else:
        logger.info(f"  J值: N/A (无效)")
    logger.info("")
    
    # 成交量指标
    current_vol = latest.get('vol', None)
    vol_max_20 = latest.get('vol_max_20', None)
    vol_ratio = latest.get('vol_ratio', None)
    
    logger.info("成交量指标:")
    if current_vol is not None and not pd.isna(current_vol):
        logger.info(f"  当前成交量: {current_vol:,.0f}")
    else:
        logger.info(f"  当前成交量: N/A (无效)")
    
    if vol_max_20 is not None and not pd.isna(vol_max_20):
        logger.info(f"  前20日最大成交量: {vol_max_20:,.0f}")
    else:
        logger.info(f"  前20日最大成交量: N/A (无效)")
    
    if vol_ratio is not None and not pd.isna(vol_ratio):
        logger.info(f"  成交量比例: {vol_ratio:.2%}")
    else:
        logger.info(f"  成交量比例: N/A (无效)")
    logger.info("")
    
    # 检查条件
    logger.info("=" * 80)
    logger.info("条件检查:")
    logger.info("=" * 80)
    
    condition1_ok = False
    condition2_ok = False
    
    # 条件1：KDJ的J值 <= 阈值
    if j_value is not None and not pd.isna(j_value):
        condition1_ok = j_value <= strategy.j_threshold
        status1 = "✓" if condition1_ok else "✗"
        logger.info(f"{status1} 条件1 (J值 <= {strategy.j_threshold}): {j_value:.2f} <= {strategy.j_threshold} = {condition1_ok}")
    else:
        logger.info(f"✗ 条件1 (J值 <= {strategy.j_threshold}): J值无效，无法判断")
    
    # 条件2：成交量比例 < 50%
    if vol_ratio is not None and not pd.isna(vol_ratio):
        condition2_ok = vol_ratio < 0.5
        status2 = "✓" if condition2_ok else "✗"
        logger.info(f"{status2} 条件2 (成交量比例 < 50%): {vol_ratio:.2%} < 50% = {condition2_ok}")
    else:
        logger.info(f"✗ 条件2 (成交量比例 < 50%): 成交量比例无效，无法判断")
    
    logger.info("")
    
    # 最终结果
    result = condition1_ok and condition2_ok
    
    logger.info("=" * 80)
    if result:
        logger.info(f"✓ 结果: 符合少妇战法策略条件！")
    else:
        logger.info(f"✗ 结果: 不符合少妇战法策略条件")
        if not condition1_ok:
            logger.info(f"  原因: 条件1不满足 (J值={j_value:.2f if j_value is not None else 'N/A'} > {strategy.j_threshold})")
        if not condition2_ok:
            logger.info(f"  原因: 条件2不满足 (成交量比例={vol_ratio:.2% if vol_ratio is not None else 'N/A'} >= 50%)")
    logger.info("=" * 80)
    
    # 使用策略方法检查
    logger.info("")
    logger.info("使用策略方法检查:")
    strategy_result = strategy.check_stock(ts_code, check_date=check_date_str)
    logger.info(f"策略检查结果: {'✓ 符合条件' if strategy_result else '✗ 不符合条件'}")
    
    # 生成解释信息
    if strategy_result:
        explanation = strategy.explain(ts_code, check_date=check_date_str)
        logger.info("")
        logger.info("策略解释信息:")
        logger.info(f"  股票代码: {explanation.get('ts_code')}")
        logger.info(f"  交易日期: {explanation.get('trade_date')}")
        logger.info(f"  收盘价: {explanation.get('close', 0):.2f}")
        logger.info(f"  原因: {explanation.get('reason', '')}")
    
    # 显示最近几天的数据（用于参考）
    logger.info("")
    logger.info("=" * 80)
    logger.info("最近10天的J值和成交量比例（用于参考）:")
    logger.info("=" * 80)
    recent = processed_df.tail(10)
    for idx, row in recent.iterrows():
        date_str = row['trade_date'].strftime('%Y-%m-%d')
        j_val = row.get('kdj_j', None)
        vol_rat = row.get('vol_ratio', None)
        j_str = f"{j_val:.2f}" if j_val is not None and not pd.isna(j_val) else "N/A"
        vol_str = f"{vol_rat:.2%}" if vol_rat is not None and not pd.isna(vol_rat) else "N/A"
        marker = " ← 检查日期" if date_str == latest_date.strftime('%Y-%m-%d') else ""
        logger.info(f"  {date_str}: J={j_str:>6}, 成交量比例={vol_str:>6}{marker}")
    
    logger.info("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='调试指定股票在指定日期是否符合少妇战法策略',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  uv run python scripts/debug_stock_date.py --ts-code 300642.SZ --date 20250929
  uv run python scripts/debug_stock_date.py --ts-code 000001.SZ --date 2025-09-29
        """
    )
    parser.add_argument(
        '--ts-code',
        type=str,
        required=True,
        help='股票代码，例如 300642.SZ'
    )
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='检查日期，格式YYYYMMDD或YYYY-MM-DD，例如 20250929 或 2025-09-29'
    )
    
    args = parser.parse_args()
    
    try:
        debug_stock_date(args.ts_code, args.date)
    except KeyboardInterrupt:
        logger.warning("用户中断执行")
    except Exception as e:
        logger.error(f"运行失败: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

