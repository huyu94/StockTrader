"""
IntradayKlineCollector 测试文件

测试分时K线数据采集器的 collect 功能（使用 akshare 实时行情接口）
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent.parent
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

import pytest
import pandas as pd
from loguru import logger
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


def test_collect(collector):
    """测试 collect 功能（无参数）"""
    # 采集数据（无参数）
    df = collector.collect()
    
    # 输出 DataFrame 信息
    print_dataframe_info(df, "采集到的原始数据（akshare 格式）")
    
    # 验证结果
    assert isinstance(df, pd.DataFrame), "应该返回 DataFrame"
    assert len(df) > 0, "应该采集到数据"
    
    # 验证是 akshare 原始数据格式（包含中文列名）
    # akshare 返回的数据应该包含"代码"、"最新价"等中文列名
    assert '代码' in df.columns or 'code' in df.columns, "应该包含股票代码列"
    assert '最新价' in df.columns or 'price' in df.columns, "应该包含价格列"
    
    print(f"✓ 成功采集 {len(df)} 条原始数据")


if __name__ == "__main__":
    """
    直接运行测试（不使用 pytest）
    """
    print("=" * 60)
    print("IntradayKlineCollector collect 功能测试")
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
        # 测试 collect 功能（无参数）
        print("\n[测试] 调用 collect() 无参数...")
        raw_data = collector.collect()
        print_dataframe_info(raw_data, "采集到的原始数据（akshare 格式）")
        
        print("\n" + "=" * 60)
        print("测试完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

