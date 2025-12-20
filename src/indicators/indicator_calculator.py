#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指标计算系统 - 批量计算和缓存功能
"""

import os
import pandas as pd
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from datetime import datetime

from src.config import DATA_PATH
from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.indicators.technical_indicators import TechnicalIndicators


class IndicatorCalculator:
    """
    指标计算器，用于批量计算股票技术指标
    """
    
    def __init__(self):
        """
        初始化指标计算器
        """
        self.indicator_path = os.path.join(DATA_PATH, "indicators")
        self.stock_data_path = os.path.join(DATA_PATH, "stock_data")
        
        # 确保指标数据目录存在
        os.makedirs(self.indicator_path, exist_ok=True)
        
        # 创建数据获取器实例
        self.fetcher = StockDailyKLineFetcher()
        
        # 创建技术指标计算器实例
        self.technical_indicators = TechnicalIndicators()
    
    def get_indicator_file_path(self, ts_code: str) -> str:
        """
        获取指标文件路径
        :param ts_code: 股票代码
        :return: 指标文件路径
        """
        return os.path.join(self.indicator_path, f"{ts_code}.csv")
    
    def load_indicator_data(self, ts_code: str) -> Optional[pd.DataFrame]:
        """
        从本地加载指标数据
        :param ts_code: 股票代码
        :return: 指标数据DataFrame，不存在则返回None
        """
        file_path = self.get_indicator_file_path(ts_code)
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                # 转换日期列为datetime格式
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                logger.debug(f"{ts_code}：从本地加载了{len(df)}条指标数据")
                return df
            except Exception as e:
                logger.error(f"加载{ts_code}指标数据失败：{e}")
                return None
        else:
            logger.debug(f"{ts_code}：本地指标数据文件不存在")
            return None
    
    def save_indicator_data(self, ts_code: str, df: pd.DataFrame) -> None:
        """
        保存指标数据到本地
        :param ts_code: 股票代码
        :param df: 指标数据DataFrame
        """
        if df is None or df.empty:
            logger.warning(f"{ts_code}：指标数据为空，跳过保存")
            return
        
        file_path = self.get_indicator_file_path(ts_code)
        try:
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            logger.debug(f"{ts_code}：指标数据已保存到本地")
        except Exception as e:
            logger.error(f"保存{ts_code}指标数据失败：{e}")
    
    def calculate_indicators_for_single_stock(self, ts_code: str, indicators: Optional[List[str]] = None, force_recalculate: bool = False) -> Optional[pd.DataFrame]:
        """
        计算单只股票的技术指标
        :param ts_code: 股票代码
        :param indicators: 要计算的指标列表，None表示计算所有指标
        :param force_recalculate: 是否强制重新计算，忽略缓存
        :return: 包含技术指标的DataFrame
        """
        logger.info(f"开始计算{ts_code}的技术指标")
        
        try:
            # 加载原始股票数据
            stock_df = self.fetcher.load_local_data(ts_code)
            if stock_df is None or stock_df.empty:
                logger.warning(f"{ts_code}：原始数据不存在或为空，跳过指标计算")
                return None
            
            # 检查是否需要重新计算
            if not force_recalculate:
                # 加载本地指标数据
                indicator_df = self.load_indicator_data(ts_code)
                if indicator_df is not None and not indicator_df.empty:
                    # 检查指标数据是否与原始数据同步
                    stock_last_date = stock_df['trade_date'].max()
                    indicator_last_date = indicator_df['trade_date'].max()
                    
                    if stock_last_date == indicator_last_date:
                        logger.info(f"{ts_code}：指标数据已是最新，无需重新计算")
                        return indicator_df
                    else:
                        logger.info(f"{ts_code}：指标数据需要更新，从{indicator_last_date}更新到{stock_last_date}")
            
            # 计算指标
            if indicators is None or len(indicators) == 0:
                # 计算所有指标
                result_df = self.technical_indicators.calculate_all_indicators(stock_df)
            else:
                # 只计算指定的指标
                result_df = stock_df.copy()
                for indicator in indicators:
                    if indicator.upper() == 'BBI':
                        result_df = self.technical_indicators.calculate_bbi(result_df)
                    elif indicator.upper() == 'MACD':
                        result_df = self.technical_indicators.calculate_macd(result_df)
                    elif indicator.upper() == 'RSI':
                        result_df = self.technical_indicators.calculate_rsi(result_df)
                    elif indicator.upper() == 'KDJ':
                        result_df = self.technical_indicators.calculate_kdj(result_df)
                    else:
                        logger.warning(f"{ts_code}：未知指标{indicator}，跳过计算")
            
            # 保存指标数据到本地
            self.save_indicator_data(ts_code, result_df)
            
            logger.info(f"{ts_code}：指标计算完成，共{len(result_df)}条数据")
            return result_df
        except Exception as e:
            logger.error(f"{ts_code}：指标计算失败：{e}")
            return None
    
    def calculate_indicators_for_multiple_stocks(self, stock_codes: List[str], indicators: Optional[List[str]] = None, force_recalculate: bool = False, max_workers: int = 10) -> Dict[str, pd.DataFrame]:
        """
        批量计算多只股票的技术指标
        :param stock_codes: 股票代码列表
        :param indicators: 要计算的指标列表，None表示计算所有指标
        :param force_recalculate: 是否强制重新计算，忽略缓存
        :param max_workers: 线程池最大工作线程数
        :return: 股票代码为键，指标数据DataFrame为值的字典
        """
        logger.info(f"开始批量计算{len(stock_codes)}只股票的技术指标")
        
        result = {}
        
        # 使用线程池并行计算
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stock = {
                executor.submit(self.calculate_indicators_for_single_stock, ts_code, indicators, force_recalculate): ts_code 
                for ts_code in stock_codes
            }
            
            for future in as_completed(future_to_stock):
                ts_code = future_to_stock[future]
                try:
                    stock_result = future.result()
                    if stock_result is not None and not stock_result.empty:
                        result[ts_code] = stock_result
                except Exception as e:
                    logger.error(f"{ts_code}：批量计算指标失败：{e}")
        
        logger.info(f"批量计算完成，成功计算{len(result)}只股票的指标")
        return result
    
    def calculate_indicators_for_all_stocks(self, indicators: Optional[List[str]] = None, force_recalculate: bool = False, max_workers: int = 10) -> Dict[str, pd.DataFrame]:
        """
        计算所有股票的技术指标
        :param indicators: 要计算的指标列表，None表示计算所有指标
        :param force_recalculate: 是否强制重新计算，忽略缓存
        :param max_workers: 线程池最大工作线程数
        :return: 股票代码为键，指标数据DataFrame为值的字典
        """
        logger.info("开始计算所有股票的技术指标")
        
        # 获取所有股票代码
        stock_codes = self.fetcher.get_all_stock_codes()
        logger.info(f"共获取到{len(stock_codes)}只股票")
        
        # 批量计算所有股票的指标
        result = self.calculate_indicators_for_multiple_stocks(stock_codes, indicators, force_recalculate, max_workers)
        
        logger.info("所有股票指标计算完成")
        return result
    
    def update_indicators(self) -> None:
        """
        更新所有股票的指标数据
        """
        logger.info("开始更新所有股票的指标数据")
        
        # 获取所有股票代码
        stock_codes = self.fetcher.get_all_stock_codes()
        logger.info(f"共获取到{len(stock_codes)}只股票")
        
        # 批量计算所有股票的指标（不强制重新计算，只更新需要的部分）
        self.calculate_indicators_for_multiple_stocks(stock_codes, None, False, 10)
        
        logger.info("所有股票指标更新完成")


# 示例用法
if __name__ == "__main__":
    calculator = IndicatorCalculator()
    
    # 计算单只股票的指标
    # df = calculator.calculate_indicators_for_single_stock('000001.SZ')
    # print(df.tail())
    
    # 计算多只股票的指标
    # result = calculator.calculate_indicators_for_multiple_stocks(['000001.SZ', '600000.SH'])
    # for ts_code, df in result.items():
    #     print(f"{ts_code}：{len(df)}条数据")
    #     print(df.tail())
    
    # 计算所有股票的指标
    # calculator.calculate_indicators_for_all_stocks()
    
    # 更新所有股票的指标
    # calculator.update_indicators()
