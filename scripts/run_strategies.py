"""
策略运行脚本

运行少妇战法策略，筛选符合条件的股票。

流程：
1. 初始化日志和Manager
2. 确保基础数据已更新
3. 创建少妇战法策略实例
4. 并行遍历所有股票，筛选符合条件的股票
5. 输出结果到终端和文件
"""

import sys
import os
import argparse
import json
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import setup_logger
from src.manager import Manager
from src.strategies import KDJStrategy
from project_var import OUTPUT_DIR

# 行业列表（按股票数量排序，前50个主要行业）
INDUSTRIES = [
    "通用设备", "元件", "化学制品", "专用机械", "化学原料",
    "汽车零部件", "半导体", "医疗器械", "化学制药", "通信设备",
    "机械基件", "计算机应用", "软件开发", "自动化设备", "食品",
    "白色家电", "中药", "银行", "证券", "房地产开发",
    "其他电子", "物流", "农业综合", "纺织", "工程机械",
    "包装印刷", "煤炭开采", "其他建材", "水泥", "电力",
    "贸易", "旅游服务", "港口", "水务", "石油加工",
    "铅锌", "园林工程", "其他专用设备", "纺织机械", "航空运输",
    "钢铁", "石油开采", "农产品加工", "白酒", "消费电子",
    "综合", "路桥", "煤炭加工", "酒店餐饮", "其他行业"
]


def list_industries():
    """
    列出所有可用的行业
    
    :return: 行业列表字符串
    """
    industries_str = "\n可用行业列表（前50个主要行业）：\n"
    industries_str += "=" * 80 + "\n"
    
    # 每行显示5个行业
    for i in range(0, len(INDUSTRIES), 5):
        row_industries = INDUSTRIES[i:i+5]
        industries_str += "  " + "  |  ".join(f"{ind:<12}" for ind in row_industries) + "\n"
    
    industries_str += "=" * 80 + "\n"
    industries_str += "\n使用示例：\n"
    industries_str += '  python scripts/run_strategies.py --industries "银行,证券"\n'
    industries_str += '  python scripts/run_strategies.py --industries "计算机应用,软件开发"\n'
    industries_str += '  python scripts/run_strategies.py --industries "半导体,元件"\n'
    
    return industries_str


def print_results(results: list, max_items: int = 50):
    """
    打印筛选结果到终端
    
    :param results: 结果列表
    :param max_items: 最多显示的项目数
    """
    if not results:
        logger.info("未找到符合条件的股票")
        return
    
    logger.info(f"\n{'='*80}")
    logger.info(f"找到 {len(results)} 只符合条件的股票（显示前{min(max_items, len(results))}只）")
    logger.info(f"{'='*80}")
    
    # 表头
    header = f"{'股票代码':<12} {'股票名称':<10} {'收盘价':<10} {'KDJ-J':<10} {'成交量比例':<12} {'原因':<20}"
    logger.info(header)
    logger.info("-" * 80)
    
    # 显示结果
    for i, result in enumerate(results[:max_items]):
        ts_code = result.get('ts_code', '')
        name = result.get('name', '')
        close = result.get('close', 0)
        kdj_j = result.get('kdj', {}).get('J', 0)
        vol_ratio = result.get('volume', {}).get('ratio', 0)
        reason = result.get('reason', '')
        
        logger.info(
            f"{ts_code:<12} {name:<10} {close:<10.2f} "
            f"{kdj_j:<10.2f} {vol_ratio:<12.2%} {reason:<20}"
        )
    
    if len(results) > max_items:
        logger.info(f"\n... 还有 {len(results) - max_items} 只股票未显示")


def save_results(results: list, filename_prefix: str = "kdj_strategy"):
    """
    保存结果到文件
    
    :param results: 结果列表
    :param filename_prefix: 文件名前缀
    :return: 保存的文件路径列表
    """
    if not results:
        logger.info("没有结果需要保存")
        return []
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    saved_paths = []
    
    # 保存为CSV
    csv_path = os.path.join(OUTPUT_DIR, f"{filename_prefix}_{timestamp}.csv")
    try:
        # 展平嵌套字典
        csv_data = []
        for result in results:
            row = {
                'ts_code': result.get('ts_code', ''),
                'name': result.get('name', ''),
                'trade_date': result.get('trade_date', ''),
                'close': result.get('close', 0),
                'kdj_k': result.get('kdj', {}).get('K', 0),
                'kdj_d': result.get('kdj', {}).get('D', 0),
                'kdj_j': result.get('kdj', {}).get('J', 0),
                'vol_current': result.get('volume', {}).get('current', 0),
                'vol_max_20d': result.get('volume', {}).get('max_20d', 0),
                'vol_ratio': result.get('volume', {}).get('ratio', 0),
                'reason': result.get('reason', '')
            }
            csv_data.append(row)
        
        df = pd.DataFrame(csv_data)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        saved_paths.append(csv_path)
        logger.info(f"结果已保存到CSV: {csv_path}")
    except Exception as e:
        logger.error(f"保存CSV失败: {e}")
    
    # 保存为JSON
    json_path = os.path.join(OUTPUT_DIR, f"{filename_prefix}_{timestamp}.json")
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        saved_paths.append(json_path)
        logger.info(f"结果已保存到JSON: {json_path}")
    except Exception as e:
        logger.error(f"保存JSON失败: {e}")
    
    return saved_paths


def run(industries: str = None, limit: int = None, workers: int = 8, min_days: int = 20):
    """
    运行少妇战法策略
    
    :param industries: 行业过滤，逗号分隔，例如 "银行,证券"
    :param limit: 限制处理的股票数量（用于测试）
    :param workers: 并行工作线程数
    :param min_days: 最少需要的交易日数据
    """
    # 1. 初始化日志
    setup_logger()
    logger.info("=" * 80)
    logger.info("开始运行少妇战法策略")
    logger.info("=" * 80)
    
    # 2. 初始化Manager和策略
    manager = Manager()
    
    # 确保基础数据已更新
    logger.info("检查基础数据...")
    manager.update_basic_info()
    
    # 创建策略实例（传入storage）
    strategy = KDJStrategy(storage=manager.daily_storage)
    logger.info(f"策略: {strategy.name}")
    
    # 3. 获取股票列表
    basic_df = manager.all_basic_info
    if basic_df is None or basic_df.empty:
        logger.error("无法获取股票基本信息，请先更新数据")
        return
    
    codes = basic_df["ts_code"].tolist()
    logger.info(f"总股票数: {len(codes)}")
    
    # 构建名称映射
    name_map = {}
    if 'ts_code' in basic_df.columns and 'name' in basic_df.columns:
        name_map = dict(zip(basic_df['ts_code'], basic_df['name']))
    
    # 行业过滤
    if industries:
        inds = [i.strip() for i in industries.split(',') if i.strip()]
        if 'industry' in basic_df.columns:
            allowed = set(
                basic_df[
                    basic_df['industry'].fillna('').apply(
                        lambda x: any(k.lower() in str(x).lower() for k in inds)
                    )
                ]['ts_code'].tolist()
            )
            codes = [c for c in codes if c in allowed]
            logger.info(f"行业过滤后股票数: {len(codes)}")
    
    # 限制数量（用于测试）
    if limit and limit > 0:
        codes = codes[:limit]
        logger.info(f"限制处理数量: {len(codes)}")
    
    # 4. 并行筛选股票
    selected = []
    
    def check_stock(ts_code: str):
        """检查单只股票是否符合条件"""
        try:
            # 检查数据是否足够
            df = manager.daily_storage.load(ts_code)
            if df is None or df.empty or len(df) < min_days:
                return None
            
            # 使用策略检查
            if strategy.check_stock(ts_code):
                # 生成解释信息
                explanation = strategy.explain(ts_code)
                # 添加股票名称
                explanation['name'] = name_map.get(ts_code, '')
                return explanation
            
            return None
        except Exception as e:
            logger.debug(f"检查 {ts_code} 时出错: {e}")
            return None
    
    # 并行执行
    logger.info(f"开始并行筛选（{workers}个线程）...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_stock, code): code for code in codes}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="筛选股票", unit="只"):
            result = future.result()
            if result:
                selected.append(result)
    
    # 5. 输出结果
    logger.info(f"\n筛选完成，共找到 {len(selected)} 只符合条件的股票")
    
    if selected:
        # 按KDJ-J值排序（从小到大，J值越小越好）
        selected.sort(key=lambda x: x.get('kdj', {}).get('J', 999))
        
        # 打印结果
        print_results(selected)
        
        # 保存结果
        saved_paths = save_results(selected, filename_prefix="少妇战法")
        for path in saved_paths:
            logger.info(f"结果文件: {path}")
    else:
        logger.info("未找到符合条件的股票")
    
    logger.info("=" * 80)
    logger.info("策略运行完成")
    logger.info("=" * 80)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='运行少妇战法策略筛选股票',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=list_industries()
    )
    parser.add_argument(
        '--industries',
        type=str,
        default=None,
        help=f'行业过滤，逗号分隔。可用行业：{", ".join(INDUSTRIES[:10])}... 等（共{len(INDUSTRIES)}个主要行业）。使用 --list-industries 查看完整列表'
    )
    parser.add_argument(
        '--list-industries',
        action='store_true',
        help='列出所有可用的行业列表'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='限制处理的股票数量（用于测试）'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=8,
        help='并行工作线程数，默认8'
    )
    parser.add_argument(
        '--min-days',
        type=int,
        default=20,
        help='最少需要的交易日数据，默认20'
    )
    
    args = parser.parse_args()
    
    # 如果只是列出行业，则输出后退出
    if args.list_industries:
        print(list_industries())
        return
    
    try:
        run(
            industries=args.industries,
            limit=args.limit,
            workers=args.workers,
            min_days=args.min_days
        )
    except KeyboardInterrupt:
        logger.warning("用户中断执行")
    except Exception as e:
        logger.error(f"运行失败: {e}")
        raise


if __name__ == "__main__":
    main()
