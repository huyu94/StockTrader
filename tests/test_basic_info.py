import pytest
import pandas as pd
import os
from unittest.mock import MagicMock, patch
from src.managers.basic_info_manager import BasicInfoManager
from src.fetchers.basic_info_fetcher import BasicInfoFetcher
from src.loaders.basic_info_loader import BasicInfoLoader

@pytest.fixture
def mock_provider():
    provider = MagicMock()
    # Mock return data for stock_basic
    mock_df = pd.DataFrame({
        'ts_code': ['000001.SZ', '600000.SH'],
        'symbol': ['000001', '600000'],
        'name': ['平安银行', '浦发银行'],
        'area': ['深圳', '上海'],
        'industry': ['银行', '银行'],
        'market': ['主板', '主板'],
        'list_date': ['19910403', '19991110'],
        'list_status': ['L', 'L'],
        'is_hs': ['S', 'H']
    })
    provider.query.return_value = mock_df
    return provider

def test_fetcher_fetch(mock_provider, tmp_path):
    # Patch the data dir to use tmp_path
    with patch.dict(os.environ, {"DATA_PATH": str(tmp_path)}):
        fetcher = BasicInfoFetcher(provider=mock_provider)
        # Override cache dir for test
        fetcher.cache_dir = str(tmp_path / "cache")
        os.makedirs(fetcher.cache_dir, exist_ok=True)
        
        df = fetcher.fetch(save_local=True)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'ts_code' in df.columns
        
        # Verify file created
        assert os.path.exists(os.path.join(fetcher.data_dir, "basic_info.csv"))
        # Verify cache created
        assert os.path.exists(os.path.join(fetcher.cache_dir, "basic_info_update.json"))

def test_loader_load(tmp_path):
    # Create dummy data
    data_dir = tmp_path
    os.makedirs(data_dir, exist_ok=True)
    csv_path = data_dir / "basic_info.csv"
    
    df_dummy = pd.DataFrame({
        'ts_code': ['000001.SZ'],
        'symbol': ['000001']
    })
    df_dummy.to_csv(csv_path, index=False)
    
    with patch.dict(os.environ, {"DATA_PATH": str(data_dir)}):
        loader = BasicInfoLoader()
        # Override cache dir
        loader.cache_dir = str(tmp_path / "cache")
        
        df = loader.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['ts_code'] == '000001.SZ'

def test_manager_integration(mock_provider, tmp_path):
    with patch.dict(os.environ, {"DATA_PATH": str(tmp_path)}):
        # Mock TushareProvider in fetcher
        with patch('src.fetchers.basic_info_fetcher.TushareProvider', return_value=mock_provider):
            manager = BasicInfoManager()
            # Override directories
            manager.fetcher.data_dir = str(tmp_path)
            manager.fetcher.cache_dir = str(tmp_path / "cache")
            manager.loader.data_dir = str(tmp_path)
            manager.loader.cache_dir = str(tmp_path / "cache")
            
            os.makedirs(manager.fetcher.cache_dir, exist_ok=True)
            
            # First call - should fetch
            df = manager.get_all_stocks()
            assert not df.empty
            assert mock_provider.query.called
            
            # Reset mock to verify second call doesn't fetch
            mock_provider.query.reset_mock()
            
            # Second call - should load from local (since we just fetched and updated cache)
            df2 = manager.get_all_stocks()
            assert not df2.empty
            assert not mock_provider.query.called
