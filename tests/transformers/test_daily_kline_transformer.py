"""
日K线转换器测试
"""
import sys
import os
from pathlib import Path
import pytest
import pandas as pd

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

# 直接导入，避免触发 core.__init__ 的循环导入
from core.transformers.daily_kline import DailyKlineTransformer
from core.common.exceptions import TransformerException


@pytest.fixture
def transformer():
    """创建 DailyKlineTransformer 实例"""
    return DailyKlineTransformer({
        'transform_rules': {
            'remove_halted': True,
            'validate_ohlc': True,
            'fill_missing': False
        }
    })


@pytest.fixture
def sample_data():
    """创建示例数据"""
    return pd.DataFrame([
        {
            'ts_code': '000001.SZ',
            'trade_date': '20240101',
            'open': 10.5,
            'high': 11.0,
            'low': 10.0,
            'close': 10.8,
            'vol': 1000000,
            'amount': 10800000
        },
        {
            'ts_code': '000001.SZ',
            'trade_date': '20240102',
            'open': 10.8,
            'high': 11.2,
            'low': 10.5,
            'close': 11.0,
            'vol': 1200000,
            'amount': 13200000
        }
    ])


class TestDailyKlineTransformer:
    """日K线转换器测试类"""
    
    def test_transform_basic(self, transformer, sample_data):
        """测试基本转换功能"""
        result = transformer.transform(sample_data)
        
        assert not result.empty
        assert len(result) == 2
        assert 'ts_code' in result.columns
        assert 'trade_date' in result.columns
        assert result['trade_date'].iloc[0] == '2024-01-01'
        assert result['trade_date'].iloc[1] == '2024-01-02'
    
    def test_transform_date_format(self, transformer, sample_data):
        """测试日期格式标准化"""
        result = transformer.transform(sample_data)
        
        # 验证日期格式为 YYYY-MM-DD
        assert all(result['trade_date'].str.match(r'\d{4}-\d{2}-\d{2}'))
    
    def test_transform_remove_halted(self, transformer):
        """测试剔除停牌数据"""
        data = pd.DataFrame([
            {
                'ts_code': '000001.SZ',
                'trade_date': '20240101',
                'open': 10.5,
                'high': 11.0,
                'low': 10.0,
                'close': 10.8,
                'vol': 1000000,
                'amount': 10800000
            },
            {
                'ts_code': '000001.SZ',
                'trade_date': '20240102',
                'open': 10.8,
                'high': 11.2,
                'low': 10.5,
                'close': 0,  # 停牌数据
                'vol': 0,  # 停牌数据
                'amount': 0,  # 停牌数据
            }
        ])
        
        result = transformer.transform(data)
        
        # 应该只保留一条数据
        assert len(result) == 1
        assert result['trade_date'].iloc[0] == '2024-01-01'
    
    def test_transform_validate_ohlc(self, transformer):
        """测试 OHLC 关系验证"""
        data = pd.DataFrame([
            {
                'ts_code': '000001.SZ',
                'trade_date': '20240101',
                'open': 10.5,
                'high': 9.0,  # high < low，异常数据
                'low': 10.0,
                'close': 10.8,
                'vol': 1000000,
                'amount': 10800000
            }
        ])
        
        result = transformer.transform(data)
        
        # 应该被过滤掉
        assert len(result) == 0
    
    def test_transform_empty_data(self, transformer):
        """测试空数据"""
        result = transformer.transform(pd.DataFrame())
        assert result.empty
    
    def test_transform_missing_columns(self, transformer):
        """测试缺少必需列"""
        data = pd.DataFrame([
            {'ts_code': '000001.SZ', 'trade_date': '20240101'}  # 缺少价格列
        ])
        
        with pytest.raises(TransformerException):
            transformer.transform(data)

