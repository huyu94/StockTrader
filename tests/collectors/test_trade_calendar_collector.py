"""
TradeCalendarCollector 测试文件

使用 pytest 测试交易日历采集器的功能
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
from core.collectors.trade_calendar import TradeCalendarCollector
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
    """创建 TradeCalendarCollector 实例（每个测试函数一个）"""
    return TradeCalendarCollector(
        config={
            "source": "tushare",
            "retry_times": 3,
            "timeout": 30
        },
        provider=provider
    )


# 标记需要 API 调用的测试
pytestmark = pytest.mark.api


def test_trade_calendar_collector_basic(collector):
    """测试基本的交易日历采集"""
    params = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "exchange": "SSE"  # 上交所
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "交易日历数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证必需的列
    required_columns = ['exchange', 'cal_date', 'is_open']
    for col in required_columns:
        assert col in df.columns, f"缺少必需的列: {col}"
    
    # 验证日期范围
    if 'cal_date' in df.columns:
        df['cal_date'] = df['cal_date'].astype(str)
        assert df['cal_date'].min() >= "20240101", "日期应该在指定范围内"
        assert df['cal_date'].max() <= "20240131", "日期应该在指定范围内"
    
    # 验证 is_open 值（应该是 0 或 1）
    if 'is_open' in df.columns:
        unique_values = df['is_open'].unique()
        assert all(val in [0, 1] for val in unique_values), "is_open 应该是 0 或 1"


def test_trade_calendar_collector_different_exchange(collector):
    """测试不同交易所的交易日历"""
    exchanges = ["SSE", "SZSE"]  # 上交所、深交所
    
    for exchange in exchanges:
        params = {
            "start_date": "2024-06-01",
            "end_date": "2024-06-30",
            "exchange": exchange
        }
        
        df = collector.collect(params)
        
        print(f"\n{exchange} 交易所交易日历: {len(df)} 条数据")
        
        assert isinstance(df, pd.DataFrame), f"{exchange} 应该返回 DataFrame"
        
        if not df.empty:
            if 'exchange' in df.columns:
                unique_exchanges = df['exchange'].unique()
                assert exchange in unique_exchanges, f"应该包含 {exchange} 交易所"


def test_trade_calendar_collector_date_format(collector):
    """测试不同日期格式的输入"""
    test_cases = [
        {"start_date": "2024-01-01", "end_date": "2024-01-31"},  # YYYY-MM-DD
        {"start_date": "20240101", "end_date": "20240131"},      # YYYYMMDD
    ]
    
    for i, date_params in enumerate(test_cases, 1):
        params = {
            "exchange": "SSE",
            **date_params
        }
        
        df = collector.collect(params)
        assert isinstance(df, pd.DataFrame), f"日期格式 {date_params} 应该返回 DataFrame"
        print(f"✓ 日期格式测试 {i} 通过: {date_params}, 获取 {len(df)} 条数据")


def test_trade_calendar_collector_sorted(collector):
    """测试交易日历是否按日期排序"""
    params = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "exchange": "SSE"
    }
    
    df = collector.collect(params)
    
    if not df.empty and 'cal_date' in df.columns:
        # 验证是否按日期排序
        df['cal_date'] = df['cal_date'].astype(str)
        sorted_dates = df['cal_date'].tolist()
        assert sorted_dates == sorted(sorted_dates), "日期应该按升序排列"


def test_trade_calendar_collector_trading_days(collector):
    """测试交易日和非交易日的统计"""
    params = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "exchange": "SSE"
    }
    
    df = collector.collect(params)
    
    if not df.empty and 'is_open' in df.columns:
        # 统计交易日数量
        trading_days = df[df['is_open'] == 1]
        non_trading_days = df[df['is_open'] == 0]
        
        print(f"\n交易日数量: {len(trading_days)}")
        print(f"非交易日数量: {len(non_trading_days)}")
        
        assert len(trading_days) > 0, "应该至少有一个交易日"
        assert len(trading_days) + len(non_trading_days) == len(df), "交易日和非交易日之和应该等于总天数"

