import sys
import os
import pytest
from datetime import date, datetime, timedelta

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.date_helper import DateHelper


class TestNormalizeToYyyyMmDd:
    """测试 normalize_to_yyyy_mm_dd 方法（返回 YYYY-MM-DD 格式）"""
    
    def test_normalize_yyyymmdd_to_yyyy_mm_dd(self):
        """测试 YYYYMMDD 格式输入，转换为 YYYY-MM-DD"""
        assert DateHelper.normalize_to_yyyy_mm_dd("20231225") == "2023-12-25"
        assert DateHelper.normalize_to_yyyy_mm_dd("20240101") == "2024-01-01"
    
    def test_normalize_yyyy_mm_dd_format(self):
        """测试 YYYY-MM-DD 格式输入，保持不变"""
        assert DateHelper.normalize_to_yyyy_mm_dd("2023-12-25") == "2023-12-25"
        assert DateHelper.normalize_to_yyyy_mm_dd("2024-01-01") == "2024-01-01"
    
    def test_normalize_with_whitespace(self):
        """测试带空格的输入"""
        assert DateHelper.normalize_to_yyyy_mm_dd("  20231225  ") == "2023-12-25"
        assert DateHelper.normalize_to_yyyy_mm_dd("  2023-12-25  ") == "2023-12-25"
    
    def test_normalize_invalid_date(self):
        """测试无效日期"""
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyy_mm_dd("20230230")  # 2月30日不存在
        
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyy_mm_dd("2023-02-30")
    
    def test_normalize_unsupported_format(self):
        """测试不支持的格式"""
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyy_mm_dd("2023/12/25")
        
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyy_mm_dd("2023122")  # 长度不对
    
    def test_normalize_empty_string(self):
        """测试空字符串"""
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyy_mm_dd("")
        
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyy_mm_dd(None)


class TestNormalizeToYyyymmdd:
    """测试 normalize_to_yyyymmdd 方法（返回 YYYYMMDD 格式）"""
    
    def test_normalize_yyyymmdd_format(self):
        """测试 YYYYMMDD 格式输入，保持不变"""
        assert DateHelper.normalize_to_yyyymmdd("20231225") == "20231225"
        assert DateHelper.normalize_to_yyyymmdd("20240101") == "20240101"
    
    def test_normalize_yyyy_mm_dd_to_yyyymmdd(self):
        """测试 YYYY-MM-DD 格式输入，转换为 YYYYMMDD"""
        assert DateHelper.normalize_to_yyyymmdd("2023-12-25") == "20231225"
        assert DateHelper.normalize_to_yyyymmdd("2024-01-01") == "20240101"
    
    def test_normalize_with_whitespace(self):
        """测试带空格的输入"""
        assert DateHelper.normalize_to_yyyymmdd("  20231225  ") == "20231225"
        assert DateHelper.normalize_to_yyyymmdd("  2023-12-25  ") == "20231225"
    
    def test_normalize_invalid_date(self):
        """测试无效日期"""
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyymmdd("20230230")  # 2月30日不存在
        
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyymmdd("2023-02-30")
    
    def test_normalize_unsupported_format(self):
        """测试不支持的格式"""
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyymmdd("2023/12/25")
        
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyymmdd("2023122")  # 长度不对
    
    def test_normalize_empty_string(self):
        """测试空字符串"""
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyymmdd("")
        
        with pytest.raises(ValueError):
            DateHelper.normalize_to_yyyymmdd(None)


class TestToday:
    """测试 today 方法（现在返回 YYYY-MM-DD 格式）"""
    
    def test_today_format(self):
        """测试返回格式"""
        result = DateHelper.today()
        assert len(result) == 10
        assert result[4] == '-' and result[7] == '-'
        # 验证是今天的日期
        expected = datetime.now().strftime('%Y-%m-%d')
        assert result == expected


class TestDaysAgo:
    """测试 days_ago 方法（现在返回 YYYY-MM-DD 格式）"""
    
    def test_days_ago_format(self):
        """测试返回格式"""
        result = DateHelper.days_ago(1)
        assert len(result) == 10
        assert result[4] == '-' and result[7] == '-'
    
    def test_days_ago_calculation(self):
        """测试日期计算"""
        result = DateHelper.days_ago(0)
        expected = datetime.now().strftime('%Y-%m-%d')
        assert result == expected
        
        result = DateHelper.days_ago(1)
        expected = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        assert result == expected
        
        result = DateHelper.days_ago(7)
        expected = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        assert result == expected


class TestParseToStr:
    """测试 parse_to_str 方法（现在返回 YYYY-MM-DD 格式）"""
    
    def test_parse_date_to_str(self):
        """测试 date 对象转换"""
        d = date(2023, 12, 25)
        result = DateHelper.parse_to_str(d)
        assert result == "2023-12-25"
    
    def test_parse_datetime_to_str(self):
        """测试 datetime 对象转换"""
        dt = datetime(2023, 12, 25, 10, 30, 45)
        result = DateHelper.parse_to_str(dt)
        assert result == "2023-12-25"
    
    def test_parse_str_to_str(self):
        """测试字符串转换"""
        assert DateHelper.parse_to_str("20231225") == "2023-12-25"
        assert DateHelper.parse_to_str("2023-12-25") == "2023-12-25"


class TestParseToDate:
    """测试 parse_to_date 方法"""
    
    def test_parse_date_to_date(self):
        """测试 date 对象转换"""
        d = date(2023, 12, 25)
        result = DateHelper.parse_to_date(d)
        assert result == d
        assert isinstance(result, date)
    
    def test_parse_datetime_to_date(self):
        """测试 datetime 对象转换"""
        dt = datetime(2023, 12, 25, 10, 30, 45)
        result = DateHelper.parse_to_date(dt)
        assert result == date(2023, 12, 25)
        assert isinstance(result, date)
    
    def test_parse_str_to_date(self):
        """测试字符串转换（支持 YYYY-MM-DD 和 YYYYMMDD）"""
        result = DateHelper.parse_to_date("2023-12-25")
        assert result == date(2023, 12, 25)
        assert isinstance(result, date)
        
        result = DateHelper.parse_to_date("20231225")
        assert result == date(2023, 12, 25)
        assert isinstance(result, date)


class TestParseToDatetime:
    """测试 parse_to_datetime 方法"""
    
    def test_parse_date_to_datetime(self):
        """测试 date 对象转换"""
        d = date(2023, 12, 25)
        result = DateHelper.parse_to_datetime(d)
        assert isinstance(result, datetime)
        assert result.date() == d
        assert result.time() == datetime.min.time()
    
    def test_parse_datetime_to_datetime(self):
        """测试 datetime 对象转换"""
        dt = datetime(2023, 12, 25, 10, 30, 45)
        result = DateHelper.parse_to_datetime(dt)
        assert result == dt
        assert isinstance(result, datetime)
    
    def test_parse_str_to_datetime(self):
        """测试字符串转换（支持 YYYY-MM-DD 和 YYYYMMDD）"""
        result = DateHelper.parse_to_datetime("2023-12-25")
        assert isinstance(result, datetime)
        assert result.date() == date(2023, 12, 25)
        assert result.time() == datetime.min.time()
        
        result = DateHelper.parse_to_datetime("20231225")
        assert isinstance(result, datetime)
        assert result.date() == date(2023, 12, 25)
        assert result.time() == datetime.min.time()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

