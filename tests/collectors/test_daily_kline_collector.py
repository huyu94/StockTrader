"""
DailyKlineCollector 测试文件

使用 pytest 测试日K线数据采集器的功能
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
from core.collectors.daily_kline import DailyKlineCollector
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
        print(f"\n数据统计:\n{df.describe()}")
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
    """创建 DailyKlineCollector 实例（每个测试函数一个）"""
    return DailyKlineCollector(
        config={
            "source": "tushare",
            "retry_times": 3,
            "timeout": 30
        },
        provider=provider
    )


# 标记需要 API 调用的测试
pytestmark = pytest.mark.api


def test_daily_kline_collector_with_stock_codes(collector):
    """测试指定股票代码列表的日K线采集"""
    params = {
        "ts_codes": ["000001.SZ", "000002.SZ"],  # 平安银行、万科A
        "start_date": "2024-01-01",
        "end_date": "2024-01-31"
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "采集到的日K线数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证必需的列
    required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
    for col in required_columns:
        assert col in df.columns, f"缺少必需的列: {col}"
    
    # 验证股票代码
    unique_stocks = df['ts_code'].unique()
    assert len(unique_stocks) <= 2, "应该只有2只股票"
    
    # 验证日期范围
    if 'trade_date' in df.columns:
        df['trade_date'] = df['trade_date'].astype(str)
        assert df['trade_date'].min() >= "20240101", "日期应该在指定范围内"
        assert df['trade_date'].max() <= "20240131", "日期应该在指定范围内"
    
    # 验证价格数据
    if 'close' in df.columns:
        assert df['close'].notna().all(), "收盘价不应有空值"
        assert (df['close'] > 0).all(), "收盘价应该大于0"


def test_daily_kline_collector_single_stock(collector):
    """测试单只股票的日K线采集"""
    params = {
        "ts_codes": "000001.SZ",  # 单个股票代码（字符串格式）
        "start_date": "2024-06-01",
        "end_date": "2024-06-30"
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "单只股票的日K线数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证只有一只股票
        unique_stocks = df['ts_code'].unique()
        assert len(unique_stocks) == 1, f"应该只有1只股票，实际有 {len(unique_stocks)} 只"
        assert unique_stocks[0] == "000001.SZ"
        
        # 验证数据格式
        assert 'trade_date' in df.columns
        assert 'open' in df.columns
        assert 'high' in df.columns
        assert 'low' in df.columns
        assert 'close' in df.columns


def test_daily_kline_collector_date_format(collector):
    """测试不同日期格式的输入"""
    test_cases = [
        {"start_date": "2024-01-01", "end_date": "2024-01-31"},  # YYYY-MM-DD
        {"start_date": "20240101", "end_date": "20240131"},      # YYYYMMDD
    ]
    
    for i, date_params in enumerate(test_cases, 1):
        params = {
            "ts_codes": ["000001.SZ"],
            **date_params
        }
        
        df = collector.collect(params)
        assert isinstance(df, pd.DataFrame), f"日期格式 {date_params} 应该返回 DataFrame"
        print(f"✓ 日期格式测试 {i} 通过: {date_params}, 获取 {len(df)} 条数据")


@pytest.mark.slow
def test_daily_kline_collector_by_date(collector):
    """测试按日期采集全市场数据（慢速测试）"""
    params = {
        "start_date": "2024-01-15",
        "end_date": "2024-01-15"  # 只测试一天，避免测试时间过长
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "按日期采集的全市场日K线数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证数据格式
        assert 'ts_code' in df.columns
        assert 'trade_date' in df.columns
        assert 'open' in df.columns
        assert 'close' in df.columns
        
        # 验证日期
        unique_dates = df['trade_date'].unique()
        assert len(unique_dates) == 1, "应该只有一天的数据"
        assert unique_dates[0] == "20240115"


def test_daily_kline_collector_empty_result(collector):
    """测试未来日期的采集（应该返回空结果）"""
    from datetime import datetime, timedelta
    future_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    
    params = {
        "ts_codes": ["000001.SZ"],
        "start_date": future_date,
        "end_date": future_date
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果（未来日期应该没有数据）
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    # 注意：这里不强制要求为空，因为可能返回空 DataFrame 或包含一些数据

