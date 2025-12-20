#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集成测试用例
"""

import os
import sys
import pytest
import pandas as pd
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher
from src.indicators.indicator_calculator import IndicatorCalculator
from src.strategies.base_strategy import HotSectorKDJStrategy
from src.strategies.result_output import StockResultOutput


class TestStockAnalysisSystem:
    """
    股票分析系统集成测试类
    """
    
    @pytest.fixture(scope="class")
    def fetcher(self):
        """
        创建数据获取器实例
        """
        return StockDailyKLineFetcher()
    
    @pytest.fixture(scope="class")
    def indicator_calculator(self):
        """
        创建指标计算器实例
        """
        return IndicatorCalculator()
    
    @pytest.fixture(scope="class")
    def strategy(self):
        """
        创建策略实例
        """
        return HotSectorKDJStrategy()
    
    @pytest.fixture(scope="class")
    def result_output(self):
        """
        创建结果输出实例
        """
        return StockResultOutput(output_dir="test_output")
    
    def test_get_stock_basic_info(self, fetcher):
        """
        测试获取股票基本信息
        """
        # 获取上交所股票基本信息
        sse_basic = fetcher.get_stock_basic_info('SSE', save_local=False)
        assert not sse_basic.empty
        assert 'ts_code' in sse_basic.columns
        assert 'name' in sse_basic.columns
        assert 'industry' in sse_basic.columns
        
        # 获取深交所股票基本信息
        szse_basic = fetcher.get_stock_basic_info('SZSE', save_local=False)
        assert not szse_basic.empty
    
    def test_get_daily_k_data(self, fetcher):
        """
        测试获取单只股票日线数据
        """
        # 获取招商银行(600036.SH)的日线数据
        df = fetcher.get_daily_k_data('600036.SH', start_date='20250101', end_date='20251231', save_local=False)
        assert not df.empty
        assert 'trade_date' in df.columns
        assert 'open' in df.columns
        assert 'close' in df.columns
        assert 'high' in df.columns
        assert 'low' in df.columns
        assert 'vol' in df.columns
        assert 'amount' in df.columns
    
    def test_indicator_calculation(self, indicator_calculator):
        """
        测试指标计算功能
        """
        # 计算单只股票的指标
        df = indicator_calculator.calculate_indicators_for_single_stock('600036.SH', None, True)
        assert not df.empty
        # 检查指标列是否存在
        assert 'BBI' in df.columns
        assert 'MACD' in df.columns
        assert 'DEA' in df.columns
        assert 'MACD_HIST' in df.columns
        assert 'RSI' in df.columns
        assert 'K' in df.columns
        assert 'D' in df.columns
        assert 'J' in df.columns
    
    def test_strategy_filtering(self, strategy, fetcher):
        """
        测试策略筛选功能
        """
        # 获取几只股票的数据进行测试
        test_stocks = ['600036.SH', '000001.SZ', '000858.SZ', '002415.SZ', '300750.SZ']
        
        stocks_data = {}
        for ts_code in test_stocks:
            df = fetcher.get_daily_k_data(ts_code, start_date='20250101', end_date='20251231', save_local=False)
            if df is not None and not df.empty:
                stocks_data[ts_code] = df
        
        # 确保有测试数据
        assert len(stocks_data) > 0
        
        # 使用策略筛选股票
        result = strategy.filter_stocks(stocks_data)
        # 结果应该是列表类型
        assert isinstance(result, list)
        
        # 使用带详细信息的筛选方法
        result_with_details = strategy.filter_stocks_with_details(stocks_data)
        assert isinstance(result_with_details, list)
    
    def test_result_output(self, result_output):
        """
        测试结果输出功能
        """
        # 创建测试数据
        test_result = [
            {
                "ts_code": "600036.SH",
                "name": "招商银行",
                "industry": "银行",
                "exchange": "SSE",
                "trade_date": datetime.now().strftime('%Y-%m-%d'),
                "close": 35.5,
                "20d_gain": 15.5,
                "kdj": {
                    "K": 15.2,
                    "D": 18.5,
                    "J": 8.6
                },
                "macd": 0.56,
                "rsi": 45.2,
                "bbi": 34.8
            },
            {
                "ts_code": "000001.SZ",
                "name": "平安银行",
                "industry": "银行",
                "exchange": "SZSE",
                "trade_date": datetime.now().strftime('%Y-%m-%d'),
                "close": 18.2,
                "20d_gain": 12.3,
                "kdj": {
                    "K": 12.8,
                    "D": 16.2,
                    "J": 6.0
                },
                "macd": 0.32,
                "rsi": 42.1,
                "bbi": 17.8
            }
        ]
        
        # 测试JSON输出
        json_path = result_output.save_as_json(test_result, filename="test_result.json")
        assert os.path.exists(json_path)
        
        # 测试CSV输出
        csv_path = result_output.save_as_csv(test_result, filename="test_result.csv")
        assert os.path.exists(csv_path)
        
        # 测试打印结果
        result_output.print_result(test_result, max_items=5)
        
        # 测试生成摘要
        summary = result_output.generate_summary(test_result)
        assert summary["total_stocks"] == 2
        assert summary["average_20d_gain"] == 13.9
        
        # 测试打印摘要
        result_output.print_summary(summary)
    
    def test_integration(self, fetcher, indicator_calculator, strategy, result_output):
        """
        测试完整的集成流程
        """
        # 1. 获取测试股票数据
        test_stocks = ['600036.SH', '000001.SZ', '000858.SZ']
        stocks_data = {}
        
        for ts_code in test_stocks:
            df = fetcher.get_daily_k_data(ts_code, start_date='20250101', end_date='20251231', save_local=False)
            if df is not None and not df.empty:
                stocks_data[ts_code] = df
        
        assert len(stocks_data) > 0
        
        # 2. 计算指标数据
        for ts_code, df in stocks_data.items():
            stocks_data[ts_code] = indicator_calculator.calculate_indicators_for_single_stock(ts_code, None, True)
        
        # 3. 使用策略筛选股票
        result = strategy.filter_stocks_with_details(stocks_data)
        
        # 4. 输出结果
        if result:
            # 保存为JSON
            result_output.save_as_json(result, filename="integration_test_result.json")
            # 保存为CSV
            result_output.save_as_csv(result, filename="integration_test_result.csv")
            # 打印结果
            result_output.print_result(result)
            # 生成并打印摘要
            summary = result_output.generate_summary(result)
            result_output.print_summary(summary)
    
    def test_cleanup(self):
        """
        测试清理函数
        """
        # 清理测试输出目录
        test_output_dir = "test_output"
        if os.path.exists(test_output_dir):
            import shutil
            shutil.rmtree(test_output_dir)
        assert not os.path.exists(test_output_dir)
