"""
BasicInfoCollector 测试文件

使用 pytest 测试股票基本信息采集器的功能
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
from core.collectors.basic_info import BasicInfoCollector
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
    """创建 BasicInfoCollector 实例（每个测试函数一个）"""
    return BasicInfoCollector(
        config={
            "source": "tushare",
            "retry_times": 3,
            "timeout": 30
        },
        provider=provider
    )


# 标记需要 API 调用的测试
pytestmark = pytest.mark.api


def test_basic_info_collector_all_stocks(collector):
    """测试采集所有股票基本信息"""
    params = {
        "list_status": "L"  # 上市状态
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "所有股票基本信息")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证必需的列
    required_columns = ['ts_code', 'symbol', 'name']
    for col in required_columns:
        assert col in df.columns, f"缺少必需的列: {col}"
    
    # 验证数据完整性
    assert df['ts_code'].notna().all(), "ts_code 不应有空值"
    assert df['name'].notna().all(), "name 不应有空值"


def test_basic_info_collector_with_stock_codes(collector):
    """测试指定股票代码列表的基本信息采集"""
    params = {
        "stock_codes": ["000001.SZ", "000002.SZ", "600000.SH"],  # 平安银行、万科A、浦发银行
        "list_status": "L"
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "指定股票的基本信息")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证股票代码
    unique_stocks = df['ts_code'].unique()
    assert len(unique_stocks) <= 3, "应该只有3只股票"
    
    # 验证包含指定的股票
    assert any("000001.SZ" in stocks or stocks == "000001.SZ" for stocks in unique_stocks), "应该包含 000001.SZ"


def test_basic_info_collector_by_exchange(collector):
    """测试按交易所采集基本信息"""
    params = {
        "exchange": "SSE",  # 上交所
        "list_status": "L"
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "上交所股票基本信息")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证交易所代码
        if 'exchange' in df.columns:
            unique_exchanges = df['exchange'].unique()
            assert "SSE" in unique_exchanges, "应该包含上交所股票"


def test_basic_info_collector_single_stock(collector):
    """测试单只股票的基本信息采集"""
    params = {
        "stock_codes": "000001.SZ",  # 单个股票代码（字符串格式）
        "list_status": "L"
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "单只股票的基本信息")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证只有一只股票
        unique_stocks = df['ts_code'].unique()
        assert len(unique_stocks) == 1, f"应该只有1只股票，实际有 {len(unique_stocks)} 只"
        assert unique_stocks[0] == "000001.SZ"
        
        # 验证基本信息字段
        assert 'name' in df.columns
        assert 'industry' in df.columns or 'market' in df.columns


def test_basic_info_collector_data_format(collector):
    """测试返回数据的格式"""
    params = {
        "stock_codes": ["000001.SZ"],
        "list_status": "L"
    }
    
    df = collector.collect(params)
    
    if not df.empty:
        # 验证列名
        assert 'ts_code' in df.columns
        assert 'symbol' in df.columns
        assert 'name' in df.columns
        
        # 验证数据类型
        assert df['ts_code'].dtype == 'object' or df['ts_code'].dtype.name == 'string'
        assert df['name'].dtype == 'object' or df['name'].dtype.name == 'string'
        
        # 验证数据完整性
        assert df['ts_code'].notna().all(), "ts_code 不应有空值"
        assert df['name'].notna().all(), "name 不应有空值"

