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


def test_adj_factor_collector_with_stock_codes(collector):
    """测试指定股票代码列表的复权因子采集"""
    params = {
        "ts_codes": ["000001.SZ", "000002.SZ"],  # 平安银行、万科A
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证必需的列
    required_columns = ['ts_code', 'trade_date', 'adj_factor']
    for col in required_columns:
        assert col in df.columns, f"缺少必需的列: {col}"
    
    # 验证股票代码
    unique_stocks = df['ts_code'].unique()
    assert len(unique_stocks) <= 2, "应该只有2只股票"
    assert "000001.SZ" in unique_stocks or "000002.SZ" in unique_stocks
    
    # 验证复权因子值
    assert df['adj_factor'].notna().all(), "所有复权因子值都不应为空"
    assert (df['adj_factor'] > 0).all(), "所有复权因子值都应该大于0"


def test_adj_factor_collector_single_stock(collector):
    """测试单只股票的复权因子采集"""
    params = {
        "ts_codes": ["000001.SZ"],  # 平安银行
    }
    
    # 采集数据
    df = collector.collect(params)
    
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


@pytest.mark.slow
def test_adj_factor_collector_without_stock_codes(collector):
    """测试不指定股票代码列表的复权因子采集（自动获取所有股票）"""
    # 注意：这个测试会获取所有股票，可能需要很长时间
    params = {}
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证数据格式
        assert 'ts_code' in df.columns or any('ts_code' in col for col in df.columns)
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns


def test_adj_factor_collector_empty_list(collector):
    """测试空股票代码列表"""
    params = {
        "ts_codes": [],
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    # 空列表应该返回空 DataFrame 或者自动获取所有股票
    # 这里根据实际行为调整断言


def test_adj_factor_collector_invalid_stock_code(collector):
    """测试无效股票代码"""
    params = {
        "ts_codes": ["INVALID.SZ"],
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    # 无效股票代码应该返回空 DataFrame
    assert df.empty, "无效股票代码应该返回空 DataFrame"


def test_adj_factor_collector_data_format(collector):
    """测试返回数据的格式"""
    params = {
        "ts_codes": ["000001.SZ"],
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