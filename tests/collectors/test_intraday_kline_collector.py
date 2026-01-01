"""
IntradayKlineCollector 测试文件

测试分时K线数据采集器的功能（使用 akshare 实时行情接口）
"""

import sys
from pathlib import Path
# 添加项目根目录到路径
project_path = Path(__file__).parent.parent.parent
print(project_path)
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

from utils.date_helper import DateHelper
import pytest
import pandas as pd
from loguru import logger
from datetime import datetime
from core.collectors.intraday_kline import IntradayKlineCollector


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


@pytest.fixture(scope="function")
def collector():
    """创建 IntradayKlineCollector 实例（每个测试函数一个）"""
    return IntradayKlineCollector(
        config={
            "source": "akshare",
            "retry_times": 3,
            "timeout": 30
        }
    )


# 标记需要 API 调用的测试
pytestmark = pytest.mark.api


def test_intraday_kline_collector_all_market(collector):
    """测试获取全市场实时行情数据"""
    params = {}
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "全市场实时行情数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证必需的列
    required_columns = ['ts_code', 'trade_date', 'time', 'datetime', 'price', 'volume', 'amount']
    for col in required_columns:
        assert col in df.columns, f"缺少必需的列: {col}"
    
    # 验证数据格式
    assert df['ts_code'].notna().all(), "股票代码不应有空值"
    assert df['price'].notna().all(), "价格不应有空值"
    assert (df['price'] > 0).all(), "价格应该大于0"
    
    # 验证 ts_code 格式
    assert df['ts_code'].str.contains(r'\.').all(), "股票代码应该是 ts_code 格式（包含.）"
    
    print(f"✓ 成功采集 {len(df)} 条全市场实时行情数据")


def test_intraday_kline_collector_with_stock_codes(collector):
    """测试指定股票代码列表的实时行情采集"""
    params = {
        "ts_codes": ["000001.SZ", "600000.SH"]  # 平安银行、浦发银行
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "指定股票的实时行情数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证股票代码
        unique_stocks = df['ts_code'].unique()
        assert len(unique_stocks) <= 2, f"应该最多2只股票，实际有 {len(unique_stocks)} 只"
        
        # 验证数据格式
        assert 'trade_date' in df.columns
        assert 'time' in df.columns
        assert 'datetime' in df.columns
        assert 'price' in df.columns
        assert 'volume' in df.columns
        assert 'amount' in df.columns
        
        print(f"✓ 成功采集 {len(df)} 条指定股票的实时行情数据")
    else:
        print("⚠ 未采集到数据（可能是股票代码不存在或市场未开盘）")


def test_intraday_kline_collector_single_stock(collector):
    """测试单只股票的实时行情采集"""
    params = {
        "ts_codes": "000001.SZ"  # 单个股票代码（字符串格式）
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "单只股票的实时行情数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证只有一只股票
        unique_stocks = df['ts_code'].unique()
        assert len(unique_stocks) == 1, f"应该只有1只股票，实际有 {len(unique_stocks)} 只"
        assert unique_stocks[0] == "000001.SZ"
        
        # 验证数据格式
        assert 'trade_date' in df.columns
        assert 'time' in df.columns
        assert 'price' in df.columns
        
        print(f"✓ 成功采集单只股票的实时行情数据")
    else:
        print("⚠ 未采集到数据（可能是股票代码不存在或市场未开盘）")


def test_intraday_kline_collector_with_date(collector):
    """测试指定日期的实时行情采集"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    params = {
        "ts_codes": ["000001.SZ"],
        "trade_date": today
    }
    
    # 采集数据
    df = collector.collect(params)
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "指定日期的实时行情数据")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    
    if not df.empty:
        # 验证日期格式
        assert 'trade_date' in df.columns
        # 日期应该是 YYYY-MM-DD 格式
        assert df['trade_date'].str.match(r'\d{4}-\d{2}-\d{2}').all(), "日期格式应该是 YYYY-MM-DD"
        
        print(f"✓ 成功采集指定日期的实时行情数据")
    else:
        print("⚠ 未采集到数据")


def test_intraday_kline_collector_data_types(collector):
    """测试数据类型和格式"""
    params = {
        "ts_codes": ["000001.SZ", "600000.SH"]
    }
    
    # 采集数据
    df = collector.collect(params)
    
    if not df.empty:
        # 验证数据类型
        assert pd.api.types.is_string_dtype(df['ts_code']), "ts_code 应该是字符串类型"
        assert pd.api.types.is_string_dtype(df['trade_date']), "trade_date 应该是字符串类型"
        assert pd.api.types.is_string_dtype(df['time']), "time 应该是字符串类型"
        assert pd.api.types.is_numeric_dtype(df['price']), "price 应该是数值类型"
        assert pd.api.types.is_integer_dtype(df['volume']) or pd.api.types.is_float_dtype(df['volume']), "volume 应该是数值类型"
        assert pd.api.types.is_numeric_dtype(df['amount']), "amount 应该是数值类型"
        
        # 验证时间格式
        assert df['time'].str.match(r'\d{2}:\d{2}:\d{2}').all(), "time 格式应该是 HH:MM:SS"
        
        print(f"✓ 数据类型验证通过")
    else:
        print("⚠ 未采集到数据，跳过数据类型验证")


def test_intraday_kline_collector_code_conversion(collector):
    """测试股票代码格式转换（6位数字 -> ts_code格式）"""
    # 测试使用6位数字代码
    params = {
        "ts_codes": ["000001", "600000"]  # 6位数字格式
    }
    
    # 采集数据
    df = collector.collect(params)
    
    if not df.empty:
        # 验证代码已转换为 ts_code 格式
        assert df['ts_code'].str.contains(r'\.').all(), "股票代码应该是 ts_code 格式（包含.）"
        
        print(f"✓ 股票代码格式转换成功")
        print(f"  转换后的代码: {df['ts_code'].unique().tolist()}")
    else:
        print("⚠ 未采集到数据")


if __name__ == "__main__":
    """
    直接运行测试（不使用 pytest）
    """
    print("=" * 60)
    print("IntradayKlineCollector 功能测试")
    print("=" * 60)
    
    # 创建采集器
    collector = IntradayKlineCollector(
        config={
            "source": "akshare",
            "retry_times": 3,
            "timeout": 30
        }
    )
    
    try:
        # 测试1: 全市场数据
        print("\n[测试1] 获取全市场实时行情数据...")
        params = {}
        df = collector.collect(params)
        print_dataframe_info(df, "全市场实时行情数据")
        
        # 测试2: 指定股票代码
        print("\n[测试2] 获取指定股票的实时行情数据...")
        params = {
            "ts_codes": ["000001.SZ", "600000.SH"]
        }
        df = collector.collect(params)
        print_dataframe_info(df, "指定股票的实时行情数据")
        
        # 测试3: 单只股票
        print("\n[测试3] 获取单只股票的实时行情数据...")
        params = {
            "ts_codes": "000001.SZ"
        }
        df = collector.collect(params)
        print_dataframe_info(df, "单只股票的实时行情数据")
        
        print("\n" + "=" * 60)
        print("所有测试完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

