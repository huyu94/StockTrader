"""
股票基本信息转换器测试
"""
import sys
import os
from pathlib import Path
import pytest
import pandas as pd

# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

from core.transformers.basic_info import BasicInfoTransformer
from core.common.exceptions import TransformerException


@pytest.fixture
def transformer():
    """创建 BasicInfoTransformer 实例"""
    return BasicInfoTransformer()


@pytest.fixture
def sample_data():
    """创建示例数据"""
    return pd.DataFrame([
        {
            'ts_code': '000001.SZ',
            'symbol': '000001',
            'name': '平安银行',
            'industry': '银行',
            'list_date': '19910403'
        }
    ])


class TestBasicInfoTransformer:
    """股票基本信息转换器测试类"""
    
    def test_transform_basic(self, transformer, sample_data):
        """测试基本转换功能"""
        result = transformer.transform(sample_data)
        
        assert not result.empty
        assert len(result) == 1
        assert 'ts_code' in result.columns
        assert 'symbol' in result.columns
        assert 'name' in result.columns
        assert result['list_date'].iloc[0] == '1991-04-03'
    
    def test_transform_date_format(self, transformer, sample_data):
        """测试日期格式标准化"""
        result = transformer.transform(sample_data)
        
        # 验证日期格式为 YYYY-MM-DD
        assert result['list_date'].iloc[0] == '1991-04-03'
    
    def test_transform_auto_generate_symbol(self, transformer):
        """测试自动生成 symbol"""
        data = pd.DataFrame([
            {
                'ts_code': '000001.SZ',
                'name': '平安银行'
                # 缺少 symbol
            }
        ])
        
        result = transformer.transform(data)
        
        # 应该自动从 ts_code 提取 symbol
        assert result['symbol'].iloc[0] == '000001'
    
    def test_transform_deduplicate(self, transformer):
        """测试数据去重"""
        data = pd.DataFrame([
            {
                'ts_code': '000001.SZ',
                'symbol': '000001',
                'name': '平安银行'
            },
            {
                'ts_code': '000001.SZ',
                'symbol': '000001',
                'name': '平安银行（更新）'
            }
        ])
        
        result = transformer.transform(data)
        
        # 应该只保留一条（keep='last'）
        assert len(result) == 1
        assert result['name'].iloc[0] == '平安银行（更新）'
    
    def test_transform_empty_data(self, transformer):
        """测试空数据"""
        result = transformer.transform(pd.DataFrame())
        assert result.empty
    
    def test_transform_missing_columns(self, transformer):
        """测试缺少必需列"""
        data = pd.DataFrame([
            {'name': '测试'}  # 缺少 ts_code
        ])
        
        with pytest.raises(TransformerException):
            transformer.transform(data)

