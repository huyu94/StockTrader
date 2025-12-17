import pytest
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

@pytest.fixture
def fetcher():
    """创建StockDailyKLineFetcher实例"""
    return StockDailyKLineFetcher()

@pytest.fixture
def sample_data():
    """创建模拟数据，包含缺口"""
    sample_dates = [
        "2025-12-01", "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05",
        "2025-12-08", "2025-12-09", "2025-12-10", 
        # 缺少 2025-12-11 至 2025-12-14
        "2025-12-15", "2025-12-16", "2025-12-17"
    ]
    
    return pd.DataFrame({
        "trade_date": pd.to_datetime(sample_dates),
        "close": [10.0 + i*0.1 for i in range(len(sample_dates))]
    })

@pytest.fixture
def complete_data(fetcher):
    """创建完整的模拟数据"""
    # 获取完整的交易日序列
    start_date = "20251201"
    end_date = "20251217"
    calendar_df = fetcher.get_trade_calendar(exchange='SSE', start_date=start_date, end_date=end_date)
    calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date'], format='%Y%m%d')
    trade_df = calendar_df[calendar_df['is_open'] == 1]
    
    return pd.DataFrame({
        "trade_date": trade_df['cal_date'].tolist(),
        "close": [10.0 + i*0.1 for i in range(len(trade_df))]
    })

@pytest.fixture
def empty_data():
    """创建空数据"""
    return pd.DataFrame(columns=['trade_date', 'close'])

class TestDetectMissingDates:
    """测试detect_missing_dates方法"""
    
    def test_with_sample_data(self, fetcher, sample_data):
        """测试使用模拟数据检测缺失日期"""
        # 调用检测方法
        missing_dates = fetcher.detect_missing_dates(
            exchange='SSE',
            start_date="20251201",
            end_date="20251217",
            df=sample_data
        )
        
        # 验证结果
        assert isinstance(missing_dates, pd.DatetimeIndex)
        assert len(missing_dates) >= 2  # 至少缺少2个交易日
        
        # 检查是否包含预期的缺失日期
        expected_missing = pd.to_datetime(["2025-12-11", "2025-12-12"])
        for date in expected_missing:
            assert date in missing_dates
    
    def test_with_empty_data(self, fetcher, empty_data):
        """测试检测空数据的情况"""
        # 调用检测方法
        missing_dates = fetcher.detect_missing_dates(
            exchange='SSE',
            start_date="20251201",
            end_date="20251217",
            df=empty_data
        )
        
        # 验证结果
        assert isinstance(missing_dates, pd.DatetimeIndex)
        # 空数据应该返回所有交易日
        assert len(missing_dates) > 0
    
    def test_with_complete_data(self, fetcher, complete_data):
        """测试检测完整数据的情况"""
        # 调用检测方法
        missing_dates = fetcher.detect_missing_dates(
            exchange='SSE',
            start_date="20251201",
            end_date="20251217",
            df=complete_data
        )
        
        # 验证结果
        assert isinstance(missing_dates, pd.DatetimeIndex)
        assert len(missing_dates) == 0  # 完整数据应该没有缺失日期
    
    def test_with_different_date_ranges(self, fetcher, sample_data):
        """测试不同日期范围的情况"""
        # 测试较短的日期范围
        missing_dates_short = fetcher.detect_missing_dates(
            exchange='SSE',
            start_date="20251201",
            end_date="20251210",
            df=sample_data
        )
        
        # 测试较长的日期范围
        missing_dates_long = fetcher.detect_missing_dates(
            exchange='SSE',
            start_date="20251125",
            end_date="20251220",
            df=sample_data
        )
        
        # 验证结果
        assert isinstance(missing_dates_short, pd.DatetimeIndex)
        assert isinstance(missing_dates_long, pd.DatetimeIndex)
        # 较长范围应该有更多或相同数量的缺失日期
        assert len(missing_dates_long) >= len(missing_dates_short)
    
    def test_without_df_parameter(self, fetcher):
        """测试不提供df参数的情况"""
        # 调用检测方法，不提供df参数（应该返回所有交易日）
        missing_dates = fetcher.detect_missing_dates(
            exchange='SSE',
            start_date="20251201",
            end_date="20251205"
        )
        
        # 验证结果
        assert isinstance(missing_dates, pd.DatetimeIndex)
        # 应该返回所有交易日（因为没有提供df）
        assert len(missing_dates) > 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])