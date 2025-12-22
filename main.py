import os, sys
import json
from src.manager import Manager
from src.strategies.kdj_strategy import KDJStrategy
from loguru import logger
import pandas as pd
from tqdm import tqdm
from config import setup_logger
from project_var import OUTPUT_DIR
setup_logger()

def batch_run() -> pd.DataFrame:
    """批量运行策略，筛选符合条件的股票"""
    manager = Manager(provider_name="tushare")
    target_date = "20251222"
    start_date = "20250201"
    end_date = "20251222"
    
    # 获取所有股票代码和基本信息
    basic_info = manager.all_basic_info
    ts_codes = basic_info["ts_code"].tolist()
    
    # 创建股票代码到名称的映射字典
    code_to_name = dict(zip(basic_info["ts_code"], basic_info["name"]))
    
    logger.info(f"开始批量检查，共 {len(ts_codes)} 只股票")
    logger.info(f"目标日期: {target_date}")
    logger.info(f"数据范围: {start_date} ~ {end_date}")
    logger.info("="*80)
    
    # 初始化策略
    kdj_strategy = KDJStrategy(
        storage=manager.daily_storage, 
        kdj_period=9, 
        vol_period=20, 
        j_threshold=5.0
    )
    
    stock_pool = []
    success_count = 0
    fail_count = 0
    
    # 遍历所有股票
    for i, ts_code in tqdm(enumerate(ts_codes, 1), total=len(ts_codes), desc="批量检查股票"):
        stock_name = code_to_name.get(ts_code, "未知")
        
        # 运行策略
        result = kdj_strategy.run(ts_code, start_date, end_date, target_date)
        
        if result["success"]:
            success_count += 1
            
            # 如果符合条件，添加到股票池
            if result["signal"] == "买入":
                stock_info = {
                    "ts_code": ts_code,
                    "name": stock_name,
                    "signal": result["signal"],
                    "target_date": result["target_date"]
                }
                stock_pool.append(stock_info)
                
                logger.info(
                    f"[{i}/{len(ts_codes)}] ✅ {ts_code} {stock_name} - "
                    f"符合条件 (信号: {result['signal']})"
                )
            else:
                logger.debug(
                    f"[{i}/{len(ts_codes)}] {ts_code} {stock_name} - "
                    f"不符合条件 (信号: {result['signal']})"
                )
        else:
            fail_count += 1
            logger.debug(
                f"[{i}/{len(ts_codes)}] ⚠ {ts_code} {stock_name} - "
                f"执行失败: {result.get('error', '未知错误')}"
            )
    
    # 输出统计结果
    logger.info("\n" + "="*80)
    logger.info("【批量检查完成】")
    logger.info("="*80)
    logger.info(f"总检查数: {len(ts_codes)}")
    logger.info(f"成功执行: {success_count}")
    logger.info(f"执行失败: {fail_count}")
    logger.info(f"符合条件: {len(stock_pool)}")
    logger.info("="*80)
    
    # 输出符合条件的股票池
    if stock_pool:
        logger.info("\n【符合条件的股票池】")
        logger.info("="*80)
        for i, stock in enumerate(stock_pool, 1):
            logger.info(
                f"{i:3d}. {stock['ts_code']:12s} {stock['name']:10s} "
                f"(信号: {stock['signal']}, 日期: {stock['target_date']})"
            )
        logger.info("="*80)
        
        res = pd.DataFrame(stock_pool, columns=["ts_code", "name", "signal", "target_date"])
    else:
        logger.info("\n没有符合条件的股票")
        res =  pd.DataFrame(columns=["ts_code", "name", "signal", "target_date"])
    
    res.to_csv(os.path.join(OUTPUT_DIR, "stock_pool.csv"), index=False)
    logger.info(f"股票池已保存到 {os.path.join(OUTPUT_DIR, 'stock_pool.csv')}")


def main():
    """主函数：使用KDJ策略检查股票"""
    
    # 初始化Manager
    manager = Manager(provider_name="tushare")
    
    # 设置股票代码和目标日期
    ts_code = "300642.SZ"
    target_date = "20250929"  # 目标检查日期
    
    # 为了计算指标，需要加载目标日期之前的一段历史数据
    start_date = "20250201"
    end_date = "20251222"
    
    logger.info(f"股票代码: {ts_code}")
    logger.info(f"检查日期: {target_date}")
    logger.info(f"数据范围: {start_date} ~ {end_date}")
    
    # ========== 使用 KDJ 策略 ==========
    
    logger.info("\n" + "="*80)
    logger.info("使用 KDJ 策略（少妇战法）")
    logger.info("="*80)
    
    # 初始化策略
    kdj_strategy = KDJStrategy(
        storage=manager.daily_storage,
        kdj_period=9,
        vol_period=20,
        j_threshold=5.0
    )
    
    # 运行策略（统一入口）
    logger.info(f"\n执行策略检查...")
    result = kdj_strategy.run(ts_code, start_date, end_date, target_date)
    
    # ========== 显示结果 ==========
    print(result)    
    if not result["success"]:
        logger.error(f"策略执行失败: {result['error']}")
        return
    



if __name__ == "__main__":
    # 需要导入pandas用于类型检查
    # main()
    stock_pool = batch_run()
