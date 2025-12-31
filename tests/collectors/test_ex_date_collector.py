"""
ExDateCollector 测试文件

使用 pytest 测试除权除息日采集器的功能
"""

import sys
import os
from pathlib import Path
import pytest
import pandas as pd

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

from loguru import logger
from core.collectors.ex_date import ExDateCollector
from src.fetch.providers import TushareProvider


def print_dataframe_info(df: pd.DataFrame, title: str = "DataFrame 信息"):
    """
    打印 DataFrame 的详细信息（辅助函数）
    
    Args:
        df: 要打印的 DataFrame
        title: 标题
    """
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    print(f"数据形状: {df.shape}")
    print(f"列名: {df.columns.tolist()}")
    if not df.empty:
        print(f"\n前10条数据:\n{df.head(10)}")
        if len(df) > 10:
            print(f"\n后10条数据:\n{df.tail(10)}")
    else:
        print("DataFrame 为空")
    print("=" * 60 + "\n")


# pytest fixtures - 共享测试资源
@pytest.fixture(scope="module")
def provider():
    """创建 TushareProvider 实例（模块级别，所有测试共享）"""
    return TushareProvider()


@pytest.fixture(scope="function")
def collector(provider):
    """创建 ExDateCollector 实例（每个测试函数一个）"""
    return ExDateCollector(
        config={
            "source": "tushare",
            "retry_times": 3,
            "timeout": 30
        },
        provider=provider
    )


# 标记需要 API 调用的测试
pytestmark = pytest.mark.api




def test_ex_date_collector_with_stock_code(collector):
    """测试指定股票代码的除权除息日采集"""
    params = {
        "ts_code": "000001.SZ"  # 平安银行
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "指定股票的除权除息日数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证股票代码
        if 'ts_code' in df.columns:
            unique_stocks = df['ts_code'].unique()
            assert "000001.SZ" in unique_stocks, "应该包含指定的股票代码"


def test_ex_date_collector_date_format(collector):
    """测试不同日期格式的输入"""
    test_cases = [
        "2024-06-14",  # YYYY-MM-DD
        "20240614",    # YYYYMMDD
    ]
    
    for i, ex_date in enumerate(test_cases, 1):
        params = {
            "ex_date": ex_date
        }
        
        df = collector.collect(params)
        assert isinstance(df, pd.DataFrame), f"日期格式 {ex_date} 应该返回 DataFrame"
        print(f"✓ 日期格式测试 {i} 通过: {ex_date}, 获取 {len(df)} 条数据")


def test_ex_date_collector_empty_result(collector):
    """测试未来日期的采集（可能返回空结果）"""
    from datetime import datetime, timedelta
    future_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    
    params = {
        "ex_date": future_date
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    # 未来日期可能没有除权除息记录，这是正常的


def test_ex_date_collector_get_ex_dates_list(collector):
    """测试 get_ex_dates_list 便捷方法"""
    stock_code = "000001.SZ"
    
    # 获取除权除息日列表（返回字符串列表）
    ex_dates_list = collector.get_ex_dates_list(stock_code)
    
    print(f"\n股票 {stock_code} 的除权除息日列表:")
    print(f"共 {len(ex_dates_list)} 个除权除息日")
    print(f"日期列表: {ex_dates_list}")
    
    # 验证结果
    assert isinstance(ex_dates_list, list), "应该返回列表"
    # 注意：如果该股票没有除权除息，列表可能为空


def test_ex_date_collector_get_single_stock_ex_dates(collector):
    """测试 get_single_stock_ex_dates 便捷方法（返回 DataFrame）"""
    stock_code = "000001.SZ"
    
    # 获取除权除息日 DataFrame
    df = collector.get_single_stock_ex_dates(stock_code)
    
    print_dataframe_info(df, f"股票 {stock_code} 的除权除息日数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    if not df.empty:
        assert 'ts_code' in df.columns, "应该包含 ts_code 列"
        assert 'ex_date' in df.columns, "应该包含 ex_date 列"
        # 验证所有数据都是指定股票的
        assert (df['ts_code'] == stock_code).all(), "所有数据应该是指定股票的"


def test_ex_date_collector_get_batch_stocks_ex_dates(collector):
    """测试 get_batch_stocks_ex_dates 便捷方法（批量获取）"""
    stock_codes = ["000001.SZ", "000002.SZ"]  # 平安银行、万科A
    
    # 批量获取除权除息日数据
    df = collector.get_batch_stocks_ex_dates(stock_codes)
    
    print_dataframe_info(df, "批量获取的除权除息日数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        assert 'ts_code' in df.columns, "应该包含 ts_code 列"
        assert 'ex_date' in df.columns, "应该包含 ex_date 列"
        
        # 验证包含指定的股票代码
        unique_stocks = df['ts_code'].unique()
        for code in stock_codes:
            assert code in unique_stocks, f"应该包含股票代码 {code}"

