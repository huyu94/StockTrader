"""
AdjFactorCollector 测试文件

使用 pytest 测试复权因子采集器的单一股票爬取功能
"""

import sys
from pathlib import Path
import pytest
import pandas as pd

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
print(project_path)
sys.path.insert(0, str(project_path))

# 直接导入，避免触发 core/__init__.py 中的批量导入
from core.collectors.adj_factor import AdjFactorCollector
from core.providers.tushare_provider import TushareProvider


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
pytestmark = pytest.mark.api

@pytest.mark.skip
def test_collect_single_stock_basic(collector):
    """测试采集单一股票的基本复权因子数据"""
    ts_code = "000001.SZ"  # 平安银行
    
    # 采集数据
    df = collector.collect(ts_code=ts_code)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, f"股票 {ts_code} 的复权因子数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert not df.empty, "应该采集到数据"
    
    # 验证必需的列
    required_columns = ['ts_code', 'trade_date', 'adj_factor']
    for col in required_columns:
        assert col in df.columns, f"缺少必需的列: {col}"
    
    # 验证股票代码
    assert (df['ts_code'] == ts_code).all(), "所有数据应该是指定股票的"
    
    # 验证复权因子值
    valid_factors = df[df['adj_factor'].notna()]
    assert len(valid_factors) > 0, "应该有有效的复权因子数据"
    assert (valid_factors['adj_factor'] > 0).all(), "所有复权因子值都应该大于0"

@pytest.mark.skip
def test_collect_single_stock_with_date_range(collector):
    """测试采集单一股票指定日期范围的复权因子数据"""
    ts_code = "000001.SZ"  # 平安银行
    start_date = "2024-01-01"
    end_date = "2024-12-31"
    
    # 采集数据
    df = collector.collect(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date
    )
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, f"股票 {ts_code} 在 {start_date} 到 {end_date} 的复权因子数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证只有一只股票
        assert (df['ts_code'] == ts_code).all(), "所有数据应该是指定股票的"
        
        # 验证日期范围
        from utils.date_helper import DateHelper
        start = pd.Timestamp(DateHelper.parse_to_date(start_date))
        end = pd.Timestamp(DateHelper.parse_to_date(end_date))
        trade_dates = pd.to_datetime(df['trade_date'], format='%Y%m%d', errors='coerce')
        
        if len(trade_dates.dropna()) > 0:
            assert trade_dates.min() >= start, "交易日期应该在指定范围内"
            assert trade_dates.max() <= end, "交易日期应该在指定范围内"


def test_collect_single_stock_date_formats(collector):
    """测试不同日期格式的输入"""
    ts_code = "000001.SZ"
    
    test_cases = [
        {"start_date": "2024-01-01", "end_date": "2024-01-31"},  # YYYY-MM-DD
        {"start_date": "20240101", "end_date": "20240131"},      # YYYYMMDD
    ]
    
    for i, date_params in enumerate(test_cases, 1):
        df = collector.collect(ts_code=ts_code, **date_params)
        assert isinstance(df, pd.DataFrame), f"日期格式测试 {i} 应该返回 DataFrame"
        print(f"✓ 日期格式测试 {i} 通过，获取 {len(df)} 条数据")

@pytest.mark.skip
def test_collect_single_stock_data_format(collector):
    """测试返回数据的格式"""
    ts_code = "000001.SZ"
    
    df = collector.collect(ts_code=ts_code)
    
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

@pytest.mark.skip
def test_collect_invalid_stock_code(collector):
    """测试无效股票代码"""
    ts_code = "INVALID.SZ"
    
    # 采集数据
    df = collector.collect(ts_code=ts_code)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    # 无效股票代码应该返回空 DataFrame
    assert df.empty, "无效股票代码应该返回空 DataFrame"

@pytest.mark.skip
def test_collect_missing_ts_code(collector):
    """测试缺少必需参数 ts_code（应该抛出异常）"""
    # 应该抛出异常，因为 ts_code 是必需参数
    with pytest.raises(TypeError):
        collector.collect()


def test_get_single_stock_adj_factor(collector):
    """测试 get_single_stock_adj_factor 便捷方法（获取除权除息日对应的复权因子）"""
    ts_code = "000001.SZ"
    
    # 获取单只股票的复权因子
    df = collector.get_single_stock_adj_factor(ts_code)
    
    # print_dataframe_info(df, f"股票 {ts_code} 的除权除息日复权因子数据")
    print(df)
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证必需的列（应该包含 ex_date 和 adj_factor 相关的列）
        assert 'ts_code' in df.columns
        # 可能包含 ex_date 或 trade_date
        assert 'adj_factor' in df.columns or any('adj' in col.lower() for col in df.columns)
        
        # 验证所有数据都是指定股票的
        if 'ts_code' in df.columns:
            assert (df['ts_code'] == ts_code).all(), "所有数据应该是指定股票的"
