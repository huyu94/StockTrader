"""
策略运行器 (StrategyRunner)

提供统一的策略运行接口，支持单股票和批量运行。
考虑MySQL并发读取的可行性，提供可选的并发支持。

MySQL并发说明：
- MySQL支持多个读连接并发读取（连接池模式）
- 并发模式下共享 Storage 实例，避免资源浪费
- Storage 的 _get_connection() 每次调用都创建新连接，是线程安全的
- 默认使用串行模式，确保稳定性
- 可选启用并发模式（已优化，共享 Storage 实例）
"""

import os
import pandas as pd
from typing import List, Dict, Any, Optional, Type
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from loguru import logger

from src.manager import Manager
from src.strategies.base_strategy import BaseStrategy
from src.storage import DailyKlineStorage
from project_var import OUTPUT_DIR


class StrategyRunner:
    """
    策略运行器
    
    功能：
    1. 单股票指定日期运行
    2. 批量指定日期运行
    3. 可选的并发支持（默认关闭）
    4. 进度显示和结果保存
    
    使用示例：
    ```python
    from src.strategies.kdj_strategy import KDJStrategy
    
    runner = StrategyRunner()
    
    # 单股票运行
    result = runner.run_single(
        strategy_class=KDJStrategy,
        ts_code="300642.SZ",
        target_date="20250929",
        start_date="20250201",
        end_date="20251222"
    )
    
    # 批量运行
    stock_pool = runner.run_batch(
        strategy_class=KDJStrategy,
        target_date="20251222",
        start_date="20250201",
        end_date="20251222",
        use_concurrent=False  # 默认关闭并发
    )
    ```
    """
    
    def __init__(self, manager: Optional[Manager] = None):
        """
        初始化策略运行器
        
        :param manager: Manager实例，如果为None则自动创建
        """
        self.manager = manager or Manager(provider_name="tushare")
        self.code_to_name = self._build_code_to_name_map()
    
    def _build_code_to_name_map(self) -> Dict[str, str]:
        """构建股票代码到名称的映射字典"""
        try:
            basic_info = self.manager.all_basic_info
            return dict(zip(basic_info["ts_code"], basic_info["name"]))
        except Exception as e:
            logger.warning(f"构建股票代码映射失败: {e}")
            return {}
    
    def run_single(
        self,
        strategy_class: Type[BaseStrategy],
        ts_code: str,
        target_date: str,
        start_date: str,
        end_date: str,
        strategy_kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        单股票指定日期运行策略
        
        :param strategy_class: 策略类（如 KDJStrategy）
        :param ts_code: 股票代码
        :param target_date: 目标检查日期（YYYYMMDD格式）
        :param start_date: 开始日期（YYYYMMDD格式）
        :param end_date: 结束日期（YYYYMMDD格式）
        :param strategy_kwargs: 策略初始化参数（如 kdj_period=9）
        :return: 策略执行结果字典
        """
        strategy_kwargs = strategy_kwargs or {}
        
        # 创建策略实例
        strategy = strategy_class(
            storage=self.manager.daily_storage,
            **strategy_kwargs
        )
        
        # 运行策略
        logger.info(f"运行策略: {strategy.name}")
        logger.info(f"股票代码: {ts_code}")
        logger.info(f"检查日期: {target_date}")
        logger.info(f"数据范围: {start_date} ~ {end_date}")
        
        result = strategy.run(ts_code, start_date, end_date, target_date)
        
        # 添加股票名称
        if result["success"]:
            stock_name = self.code_to_name.get(ts_code, "未知")
            result["stock_name"] = stock_name
        
        return result
    
    def run_batch(
        self,
        strategy_class: Type[BaseStrategy],
        target_date: str,
        start_date: str,
        end_date: str,
        strategy_kwargs: Optional[Dict[str, Any]] = None,
        ts_codes: Optional[List[str]] = None,
        use_concurrent: bool = False,
        max_workers: int = 4,
        save_results: bool = True,
        output_filename: str = "stock_pool"
    ) -> pd.DataFrame:
        """
        批量运行策略，筛选符合条件的股票
        
        :param strategy_class: 策略类（如 KDJStrategy）
        :param target_date: 目标检查日期（YYYYMMDD格式）
        :param start_date: 开始日期（YYYYMMDD格式）
        :param end_date: 结束日期（YYYYMMDD格式）
        :param strategy_kwargs: 策略初始化参数
        :param ts_codes: 股票代码列表，如果为None则使用所有股票
        :param use_concurrent: 是否使用并发（默认False）
        :param max_workers: 并发时的最大工作线程数（仅在use_concurrent=True时有效）
        :param save_results: 是否保存结果到文件
        :param output_filename: 输出文件名（不含扩展名）
        :return: 符合条件的股票DataFrame
        """
        strategy_kwargs = strategy_kwargs or {}
        
        # 获取股票列表
        if ts_codes is None:
            basic_info = self.manager.all_basic_info
            ts_codes = basic_info["ts_code"].tolist()
        
        logger.info("="*80)
        logger.info("【批量策略运行】")
        logger.info("="*80)
        logger.info(f"策略: {strategy_class.__name__}")
        logger.info(f"目标日期: {target_date}")
        logger.info(f"数据范围: {start_date} ~ {end_date}")
        logger.info(f"股票数量: {len(ts_codes)}")
        logger.info(f"并发模式: {'启用' if use_concurrent else '关闭'}")
        if use_concurrent:
            logger.info(f"并发线程数: {max_workers}")
        logger.info("="*80)
        
        stock_pool = []
        success_count = 0
        fail_count = 0
        
        if use_concurrent:
            # 并发模式
            stock_pool = self._run_batch_concurrent(
                strategy_class=strategy_class,
                ts_codes=ts_codes,
                target_date=target_date,
                start_date=start_date,
                end_date=end_date,
                strategy_kwargs=strategy_kwargs,
                max_workers=max_workers
            )
            success_count = len([s for s in stock_pool if s.get("success")])
            fail_count = len(ts_codes) - success_count
        else:
            # 串行模式
            stock_pool = self._run_batch_serial(
                strategy_class=strategy_class,
                ts_codes=ts_codes,
                target_date=target_date,
                start_date=start_date,
                end_date=end_date,
                strategy_kwargs=strategy_kwargs
            )
            success_count = len([s for s in stock_pool if s.get("success")])
            fail_count = len(ts_codes) - success_count
        
        # 筛选符合条件的股票（signal == "买入"）
        matched_stocks = [
            {
                "ts_code": s["ts_code"],
                "name": s.get("stock_name", self.code_to_name.get(s["ts_code"], "未知")),
                "signal": s["signal"],
                "target_date": s["target_date"]
            }
            for s in stock_pool
            if s.get("success") and s.get("signal") == "买入"
        ]
        
        # 输出统计结果
        logger.info("\n" + "="*80)
        logger.info("【批量运行完成】")
        logger.info("="*80)
        logger.info(f"总检查数: {len(ts_codes)}")
        logger.info(f"成功执行: {success_count}")
        logger.info(f"执行失败: {fail_count}")
        logger.info(f"符合条件: {len(matched_stocks)}")
        logger.info("="*80)
        
        # 输出符合条件的股票池
        if matched_stocks:
            logger.info("\n【符合条件的股票池】")
            logger.info("="*80)
            for i, stock in enumerate(matched_stocks, 1):
                logger.info(
                    f"{i:3d}. {stock['ts_code']:12s} {stock['name']:10s} "
                    f"(信号: {stock['signal']}, 日期: {stock['target_date']})"
                )
            logger.info("="*80)
        else:
            logger.info("\n没有符合条件的股票")
        
        # 转换为DataFrame
        if matched_stocks:
            df = pd.DataFrame(matched_stocks, columns=["ts_code", "name", "signal", "target_date"])
        else:
            df = pd.DataFrame(columns=["ts_code", "name", "signal", "target_date"])
        
        # 保存结果
        if save_results:
            self._save_results(df, output_filename)
        
        return df
    
    def _run_batch_serial(
        self,
        strategy_class: Type[BaseStrategy],
        ts_codes: List[str],
        target_date: str,
        start_date: str,
        end_date: str,
        strategy_kwargs: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """串行模式运行批量策略"""
        stock_pool = []
        
        # 创建策略实例（共享）
        strategy = strategy_class(
            storage=self.manager.daily_storage,
            **strategy_kwargs
        )
        
        # 遍历所有股票
        for i, ts_code in enumerate(tqdm(ts_codes, desc="批量检查股票"), 1):
            stock_name = self.code_to_name.get(ts_code, "未知")
            
            # 运行策略
            result = strategy.run(ts_code, start_date, end_date, target_date)
            result["ts_code"] = ts_code
            result["stock_name"] = stock_name
            stock_pool.append(result)
            
            # 记录日志
            if result["success"]:
                if result["signal"] == "买入":
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
                logger.debug(
                    f"[{i}/{len(ts_codes)}] ⚠ {ts_code} {stock_name} - "
                    f"执行失败: {result.get('error', '未知错误')}"
                )
        
        return stock_pool
    
    def _run_batch_concurrent(
        self,
        strategy_class: Type[BaseStrategy],
        ts_codes: List[str],
        target_date: str,
        start_date: str,
        end_date: str,
        strategy_kwargs: Dict[str, Any],
        max_workers: int = 4
    ) -> List[Dict[str, Any]]:
        """
        并发模式运行批量策略
        
        优化：共享 Storage 实例，避免每个线程创建 Manager
        - MySQL 在连接池模式下支持多个读连接并发读取
        - Storage 的 _get_connection() 每次调用都创建新连接，是线程安全的
        - 所有线程共享同一个 daily_storage 实例，大幅减少资源占用
        - 4个并发线程只需要 1 个 Storage 实例，而不是 4 个 Manager（12个Storage）
        """
        stock_pool = []
        
        # 创建共享的 Storage 实例（只创建一次，所有线程共享）
        shared_storage = DailyKlineStorage()
        
        def run_single_stock(ts_code: str) -> Dict[str, Any]:
            """在单个线程中运行策略"""
            try:
                # 使用共享的 Storage 实例，每个线程只创建策略实例
                # MySQL 的 _get_session() 每次创建新会话，支持并发读取
                thread_strategy = strategy_class(
                    storage=shared_storage,  # 共享 Storage 实例
                    **strategy_kwargs
                )
                
                # 运行策略
                result = thread_strategy.run(ts_code, start_date, end_date, target_date)
                result["ts_code"] = ts_code
                result["stock_name"] = self.code_to_name.get(ts_code, "未知")
                
                return result
            except Exception as e:
                logger.error(f"并发执行失败 {ts_code}: {e}")
                return {
                    "ts_code": ts_code,
                    "stock_name": self.code_to_name.get(ts_code, "未知"),
                    "success": False,
                    "error": str(e),
                    "signal": "观望"
                }
        
        # 使用线程池并发执行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_code = {
                executor.submit(run_single_stock, ts_code): ts_code
                for ts_code in ts_codes
            }
            
            # 收集结果（带进度条）
            with tqdm(total=len(ts_codes), desc="批量检查股票（并发）") as pbar:
                for future in as_completed(future_to_code):
                    ts_code = future_to_code[future]
                    try:
                        result = future.result()
                        stock_pool.append(result)
                        
                        # 记录日志
                        if result["success"]:
                            if result["signal"] == "买入":
                                logger.info(
                                    f"✅ {ts_code} {result['stock_name']} - "
                                    f"符合条件 (信号: {result['signal']})"
                                )
                    except Exception as e:
                        logger.error(f"获取结果失败 {ts_code}: {e}")
                        stock_pool.append({
                            "ts_code": ts_code,
                            "stock_name": self.code_to_name.get(ts_code, "未知"),
                            "success": False,
                            "error": str(e),
                            "signal": "观望"
                        })
                    finally:
                        pbar.update(1)
        
        # 按原始顺序排序（可选）
        code_to_index = {code: i for i, code in enumerate(ts_codes)}
        stock_pool.sort(key=lambda x: code_to_index.get(x["ts_code"], 999999))
        
        return stock_pool
    
    def _save_results(self, df: pd.DataFrame, filename: str):
        """保存结果到CSV和JSON文件"""
        if df.empty:
            logger.warning("没有数据需要保存")
            return
        
        # 保存CSV
        csv_path = os.path.join(OUTPUT_DIR, f"{filename}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"股票池已保存到 CSV: {csv_path}")
        
        # 保存JSON
        json_path = os.path.join(OUTPUT_DIR, f"{filename}.json")
        df.to_json(json_path, orient="records", force_ascii=False, indent=4)
        logger.info(f"股票池已保存到 JSON: {json_path}")

