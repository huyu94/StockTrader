"""
交易日历转换器测试
"""
import sys
import os
from pathlib import Path
import pytest
import pandas as pd

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

from core.transformers.trade_calendar import TradeCalendarTransformer
from core.models.calendar import TradeCalendar
from core.common.exceptions import TransformerException


@pytest.fixture
def transformer():
    """创建 TradeCalendarTransformer 实例"""
    return TradeCalendarTransformer()


@pytest.fixture
def sample_data():
    """创建示例数据（长格式）"""
    return pd.DataFrame([
        {'exchange': 'SSE', 'cal_date': '20240101', 'is_open': 1},
        {'exchange': 'SZSE', 'cal_date': '20240101', 'is_open': 1},
        {'exchange': 'CFFEX', 'cal_date': '20240101', 'is_open': 0},
        {'exchange': 'SSE', 'cal_date': '20240102', 'is_open': 0},
        {'exchange': 'SZSE', 'cal_date': '20240102', 'is_open': 0},
    ])


class TestTradeCalendarTransformer:
    """交易日历转换器测试类"""
    
    def test_transform_basic(self, transformer, sample_data):
        """测试基本转换功能（长格式转宽格式）"""
        result = transformer.transform(sample_data)
        
        assert not result.empty
        assert len(result) == 2  # 2个日期
        assert 'cal_date' in result.columns
        assert 'sse_open' in result.columns
        assert 'szse_open' in result.columns
        assert 'cffex_open' in result.columns
    
    def test_transform_date_format(self, transformer, sample_data):
        """测试日期格式标准化"""
        result = transformer.transform(sample_data)
        
        # 验证日期格式为 YYYY-MM-DD
        assert result['cal_date'].iloc[0] == '2024-01-01'
        assert result['cal_date'].iloc[1] == '2024-01-02'
    
    def test_transform_wide_format(self, transformer, sample_data):
        """测试宽格式转换"""
        result = transformer.transform(sample_data)
        
        # 验证每个日期都有所有交易所的列
        required_columns = ['cal_date', 'sse_open', 'szse_open', 'cffex_open', 
                           'shfe_open', 'czce_open', 'dce_open', 'ine_open']
        assert all(col in result.columns for col in required_columns)
        
        # 验证第一天的数据
        day1 = result[result['cal_date'] == '2024-01-01'].iloc[0]
        assert day1['sse_open'] == True
        assert day1['szse_open'] == True
        assert day1['cffex_open'] == False
        
        # 验证第二天的数据
        day2 = result[result['cal_date'] == '2024-01-02'].iloc[0]
        assert day2['sse_open'] == False
        assert day2['szse_open'] == False
    
    def test_transform_to_model(self, transformer, sample_data):
        """测试转换为 TradeCalendar 模型"""
        result = transformer.transform(sample_data)
        
        # 应该可以转换为 TradeCalendar 对象
        calendars = TradeCalendar.from_dataframe(result)
        assert len(calendars) == 2
        
        # 验证第一个对象
        cal1 = calendars[0]
        assert cal1.cal_date == '2024-01-01'
        assert cal1.sse_open == True
        assert cal1.szse_open == True
        assert cal1.cffex_open == False
    
    def test_transform_single_exchange(self, transformer):
        """测试单个交易所数据"""
        data = pd.DataFrame([
            {'exchange': 'SSE', 'cal_date': '20240101', 'is_open': 1}
        ])
        
        result = transformer.transform(data)
        
        assert len(result) == 1
        assert result['sse_open'].iloc[0] == True
        assert result['szse_open'].iloc[0] == False  # 没有数据，默认为 False
    
    def test_transform_deduplicate(self, transformer):
        """测试数据去重"""
        data = pd.DataFrame([
            {'exchange': 'SSE', 'cal_date': '20240101', 'is_open': 1},
            {'exchange': 'SSE', 'cal_date': '20240101', 'is_open': 0},  # 重复
        ])
        
        result = transformer.transform(data)
        
        # 应该只保留一条（keep='last'）
        assert len(result) == 1
        assert result['sse_open'].iloc[0] == False
    
    def test_transform_empty_data(self, transformer):
        """测试空数据"""
        result = transformer.transform(pd.DataFrame())
        
        # 应该返回包含所有必需列的空 DataFrame
        assert result.empty
        required_columns = ['cal_date', 'sse_open', 'szse_open', 'cffex_open', 
                           'shfe_open', 'czce_open', 'dce_open', 'ine_open']
        assert all(col in result.columns for col in required_columns)
    
    def test_transform_missing_columns(self, transformer):
        """测试缺少必需列"""
        data = pd.DataFrame([
            {'cal_date': '20240101'}  # 缺少 exchange 和 is_open
        ])
        
        with pytest.raises(TransformerException):
            transformer.transform(data)

