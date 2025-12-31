"""
复权因子转换器测试
"""
import sys
import os
from pathlib import Path
import pytest
import pandas as pd

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

from core.transformers.adj_factor import AdjFactorTransformer
from core.common.exceptions import TransformerException


@pytest.fixture
def transformer():
    """创建 AdjFactorTransformer 实例"""
    return AdjFactorTransformer()


@pytest.fixture
def sample_data():
    """创建示例数据"""
    return pd.DataFrame([
        {
            'ts_code': '000001.SZ',
            'trade_date': '20240101',
            'adj_factor': 1.5
        },
        {
            'ts_code': '000001.SZ',
            'trade_date': '20240102',
            'adj_factor': 1.6
        }
    ])


class TestAdjFactorTransformer:
    """复权因子转换器测试类"""
    
    def test_transform_basic(self, transformer, sample_data):
        """测试基本转换功能"""
        result = transformer.transform(sample_data)
        
        assert not result.empty
        assert len(result) == 2
        assert 'ts_code' in result.columns
        assert 'trade_date' in result.columns
        assert 'adj_factor' in result.columns
        assert result['trade_date'].iloc[0] == '2024-01-01'
    
    def test_transform_date_format(self, transformer, sample_data):
        """测试日期格式标准化"""
        result = transformer.transform(sample_data)
        
        # 验证日期格式为 YYYY-MM-DD
        assert all(result['trade_date'].str.match(r'\d{4}-\d{2}-\d{2}'))
    
    def test_transform_filter_invalid(self, transformer):
        """测试过滤无效数据（adj_factor <= 0）"""
        data = pd.DataFrame([
            {
                'ts_code': '000001.SZ',
                'trade_date': '20240101',
                'adj_factor': 1.5
            },
            {
                'ts_code': '000001.SZ',
                'trade_date': '20240102',
                'adj_factor': -1.0  # 无效数据
            },
            {
                'ts_code': '000001.SZ',
                'trade_date': '20240103',
                'adj_factor': 0  # 无效数据
            }
        ])
        
        result = transformer.transform(data)
        
        # 应该只保留一条有效数据
        assert len(result) == 1
        assert result['trade_date'].iloc[0] == '2024-01-01'
    
    def test_transform_empty_data(self, transformer):
        """测试空数据"""
        result = transformer.transform(pd.DataFrame())
        assert result.empty
    
    def test_transform_missing_columns(self, transformer):
        """测试缺少必需列"""
        data = pd.DataFrame([
            {'ts_code': '000001.SZ'}  # 缺少 trade_date 和 adj_factor
        ])
        
        with pytest.raises(TransformerException):
            transformer.transform(data)

