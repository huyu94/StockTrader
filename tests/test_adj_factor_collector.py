"""
AdjFactorCollector 测试文件

使用 pytest 测试复权因子采集器的功能
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
from core.collectors.adj_factor import AdjFactorCollector
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
        print(f"\n数据类型:\n{df.dtypes}")
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
    """创建 AdjFactorCollector 实例（每个测试函数一个）"""
    return AdjFactorCollector(
        config={
            "source": "tushare",
            "retry_times": 3,
            "timeout": 30
        },
        provider=provider
    )


# 标记需要 API 调用的测试
# 使用 pytestmark 为模块中所有测试函数添加标记
pytestmark = pytest.mark.api  # 所有测试都需要 API


def test_adj_factor_collector_with_stock_code(collector):
    """测试指定股票代码的复权因子采集"""
    params = {
        "ts_code": "000001.SZ",  # 平安银行
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息（使用 -s 参数运行 pytest 可以看到）
    print_dataframe_info(df, "采集到的复权因子数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证必需的列
        required_columns = ['ts_code', 'trade_date', 'adj_factor']
        for col in required_columns:
            assert col in df.columns, f"缺少必需的列: {col}"
        
        # 验证股票代码
        unique_stocks = df['ts_code'].unique()
        assert "000001.SZ" in unique_stocks, "应该包含指定的股票代码"
        
        # 验证复权因子值
        valid_factors = df[df['adj_factor'].notna()]
        assert len(valid_factors) > 0, "应该有有效的复权因子数据"
        assert (valid_factors['adj_factor'] > 0).all(), "所有复权因子值都应该大于0"


def test_adj_factor_collector_with_date_range(collector):
    """测试指定日期范围的复权因子采集"""
    params = {
        "ts_code": "000001.SZ",  # 平安银行
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "指定日期范围的复权因子数据")

    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"

    if not df.empty:
        # 验证只有一只股票
        unique_stocks = df['ts_code'].unique()
        assert len(unique_stocks) == 1, f"应该只有1只股票，实际有 {len(unique_stocks)} 只"
        assert unique_stocks[0] == "000001.SZ"
        
        # 验证数据格式
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
        
        # 验证复权因子值
        valid_data = df[df['adj_factor'].notna()]
        assert len(valid_data) > 0, "应该有有效的复权因子数据"
        
        # 验证日期范围
        from utils.date_helper import DateHelper
        start = pd.Timestamp(DateHelper.parse_to_date("2024-01-01"))
        end = pd.Timestamp(DateHelper.parse_to_date("2024-12-31"))
        trade_dates = pd.to_datetime(df['trade_date'], format='%Y%m%d', errors='coerce')
        assert trade_dates.min() >= start, "交易日期应该在指定范围内"
        assert trade_dates.max() <= end, "交易日期应该在指定范围内"

    


def test_adj_factor_collector_with_trade_date(collector):
    """测试指定交易日期的复权因子采集（获取当日所有股票的复权因子）"""
    params = {
        "trade_date": "2024-06-14",  # 指定日期
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "指定交易日期的复权因子数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证数据格式
        assert 'ts_code' in df.columns
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
        
        # 验证所有数据的交易日期都是指定的日期
        assert (df['trade_date'] == "20240614").all(), "所有数据的交易日期应该是指定日期"


def test_adj_factor_collector_invalid_stock_code(collector):
    """测试无效股票代码"""
    params = {
        "ts_code": "INVALID.SZ",
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    # 无效股票代码应该返回空 DataFrame
    assert df.empty, "无效股票代码应该返回空 DataFrame"


def test_adj_factor_collector_no_params(collector):
    """测试不提供任何参数（应该抛出异常）"""
    params = {}
    
    # 应该抛出异常
    with pytest.raises(Exception):  # CollectorException
        collector.collect(params)


def test_adj_factor_collector_data_format(collector):
    """测试返回数据的格式"""
    params = {
        "ts_code": "000001.SZ",
    }
    
    df = collector.collect(params)
    
    if not df.empty:
        # 验证列名
        assert 'ts_code' in df.columns
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
        
        # 验证数据类型
        assert df['ts_code'].dtype == 'object' or df['ts_code'].dtype.name == 'string'
        assert df['trade_date'].dtype == 'object' or df['trade_date'].dtype.name == 'string'
        assert pd.api.types.is_numeric_dtype(df['adj_factor']), "adj_factor 应该是数值类型"
        
        # 验证数据完整性
        assert df['ts_code'].notna().all(), "ts_code 不应有空值"
        assert df['trade_date'].notna().all(), "trade_date 不应有空值"
        assert df['adj_factor'].notna().any(), "至少应该有一个有效的 adj_factor 值"


def test_adj_factor_collector_date_formats(collector):
    """测试不同日期格式的输入"""
    test_cases = [
        {"ts_code": "000001.SZ", "start_date": "2024-01-01", "end_date": "2024-01-31"},  # YYYY-MM-DD
        {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240131"},      # YYYYMMDD
    ]
    
    for i, params in enumerate(test_cases, 1):
        df = collector.collect(params)
        assert isinstance(df, pd.DataFrame), f"日期格式测试 {i} 应该返回 DataFrame"
        print(f"✓ 日期格式测试 {i} 通过，获取 {len(df)} 条数据")


def test_adj_factor_collector_get_single_stock_adj_factor(collector):
    """测试 get_single_stock_adj_factor 便捷方法"""
    stock_code = "000001.SZ"
    
    # 获取单只股票的复权因子
    df = collector.get_single_stock_adj_factor(stock_code)
    
    print_dataframe_info(df, f"股票 {stock_code} 的复权因子数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证必需的列
        assert 'ts_code' in df.columns
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
        
        # 验证所有数据都是指定股票的
        assert (df['ts_code'] == stock_code).all(), "所有数据应该是指定股票的"
        
        # 验证复权因子值
        valid_factors = df[df['adj_factor'].notna()]
        assert len(valid_factors) > 0, "应该有有效的复权因子数据"


def test_adj_factor_collector_get_batch_stocks_adj_factor(collector):
    """测试 get_batch_stocks_adj_factor 便捷方法（批量获取）"""
    stock_codes = ["000001.SZ", "000002.SZ"]  # 平安银行、万科A
    
    # 批量获取复权因子数据
    df = collector.get_batch_stocks_adj_factor(stock_codes)
    
    print_dataframe_info(df, "批量获取的复权因子数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证必需的列
        assert 'ts_code' in df.columns
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
        
        # 验证包含指定的股票代码
        unique_stocks = df['ts_code'].unique()
        for code in stock_codes:
            assert code in unique_stocks, f"应该包含股票代码 {code}"
        
        # 验证数据完整性
        assert df['ts_code'].notna().all(), "ts_code 不应有空值"
        assert df['trade_date'].notna().all(), "trade_date 不应有空值"