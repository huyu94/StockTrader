import os
import shutil
import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock
from src.managers.adj_factor_manager import AdjFactorManager
from src.fetchers.adj_factor_fetcher import AdjFactorFetcher

@pytest.fixture
def mock_env(tmp_path):
    data_dir = tmp_path / "data"
    cache_dir = tmp_path / "cache"
    os.environ["DATA_PATH"] = str(data_dir)
    return data_dir, cache_dir

def test_adj_factor_manager_auto_update(mock_env, monkeypatch):
    data_dir, cache_dir = mock_env
    ts_code = "000001.SZ"
    
    # Mock data
    mock_df = pd.DataFrame({
        "ts_code": [ts_code],
        "trade_date": ["20250101"],
        "adj_factor": [1.0]
    })
    
    with pytest.MonkeyPatch.context() as m:
        manager = AdjFactorManager()
        # Redirect directories
        manager.loader.data_dir = str(data_dir)
        manager.loader.factor_dir = os.path.join(str(data_dir), "adj_factor")
        manager.loader.cache_dir = str(cache_dir)
        manager.loader.cache_file = os.path.join(str(cache_dir), "adj_factor_cache.csv")
        
        manager.fetcher.data_dir = str(data_dir)
        manager.fetcher.factor_dir = os.path.join(str(data_dir), "adj_factor")
        manager.fetcher.cache_dir = str(cache_dir)
        manager.fetcher.cache_file = os.path.join(str(cache_dir), "adj_factor_cache.csv")
        
        os.makedirs(manager.loader.factor_dir, exist_ok=True)
        os.makedirs(manager.loader.cache_dir, exist_ok=True)
        
        # Mock fetcher.fetch_one
        manager.fetcher.provider.query = MagicMock(return_value=mock_df)
        
        # 1. No local data -> Trigger update
        df = manager.get_adj_factor(ts_code)
        assert df is not None
        assert df.iloc[0]["trade_date"] == "20250101"
        manager.fetcher.provider.query.assert_called()
        
        # Verify cache file created
        assert os.path.exists(manager.loader.cache_file)
        cache_df = pd.read_csv(manager.loader.cache_file)
        assert ts_code in cache_df["ts_code"].values
        
        # 2. Data exists and cache fresh (simulate by not changing anything, since we just updated it)
        manager.fetcher.provider.query.reset_mock()
        
        # Re-load to refresh cache map
        manager.loader._load_cache_file()
        
        df2 = manager.get_adj_factor(ts_code)
        assert df2 is not None
        # Should NOT trigger fetch query
        manager.fetcher.provider.query.assert_not_called()
        
        # 3. Simulate cache expired
        # Rewrite cache file with old date
        old_date = "2000-01-01 00:00:00"
        cache_df.loc[cache_df["ts_code"] == ts_code, "last_updated_at"] = old_date
        cache_df.to_csv(manager.loader.cache_file, index=False)
        
        # Force reload cache map in loader (in real usage check_update_needed does this)
        # manager.loader.check_update_needed calls _load_cache_file
        
        df3 = manager.get_adj_factor(ts_code)
        # Should trigger fetch again
        manager.fetcher.provider.query.assert_called()

def test_batch_get_adj_factors(mock_env):
    data_dir, cache_dir = mock_env
    ts_codes = ["000001.SZ", "000002.SZ"]
    
    mock_df1 = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20250101"], "adj_factor": [1.0]})
    mock_df2 = pd.DataFrame({"ts_code": ["000002.SZ"], "trade_date": ["20250101"], "adj_factor": [2.0]})
    
    with pytest.MonkeyPatch.context() as m:
        manager = AdjFactorManager()
        # Redirect directories
        manager.loader.data_dir = str(data_dir)
        manager.loader.factor_dir = os.path.join(str(data_dir), "adj_factor")
        manager.loader.cache_dir = str(cache_dir)
        manager.loader.cache_file = os.path.join(str(cache_dir), "adj_factor_cache.csv")
        
        manager.fetcher.data_dir = str(data_dir)
        manager.fetcher.factor_dir = os.path.join(str(data_dir), "adj_factor")
        manager.fetcher.cache_dir = str(cache_dir)
        manager.fetcher.cache_file = os.path.join(str(cache_dir), "adj_factor_cache.csv")
        
        os.makedirs(manager.loader.factor_dir, exist_ok=True)
        os.makedirs(manager.loader.cache_dir, exist_ok=True)
        
        # Mock fetcher.fetch_one to return different DF based on ts_code arg
        def side_effect(ts_code, **kwargs):
            if ts_code == "000001.SZ":
                return mock_df1
            return mock_df2
            
        manager.fetcher.fetch_one = MagicMock(side_effect=side_effect)
        
        results = manager.batch_get_adj_factors(ts_codes)
        
        assert len(results) == 2
        assert results["000001.SZ"].equals(mock_df1)
        assert results["000002.SZ"].equals(mock_df2)
        assert manager.fetcher.fetch_one.call_count == 2

def test_fetcher_update_cache_new_entry(mock_env):
    data_dir, cache_dir = mock_env
    fetcher = AdjFactorFetcher(provider_name="tushare")
    fetcher.cache_dir = str(cache_dir)
    fetcher.cache_file = os.path.join(str(cache_dir), "adj_factor_cache.csv")
    os.makedirs(str(cache_dir), exist_ok=True)
    
    # 1. Update new entry
    fetcher._update_cache("000001.SZ", "2025-01-01 10:00:00")
    
    assert os.path.exists(fetcher.cache_file)
    df = pd.read_csv(fetcher.cache_file)
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "000001.SZ"
    assert df.iloc[0]["last_updated_at"] == "2025-01-01 10:00:00"
    
    # 2. Update existing entry
    fetcher._update_cache("000001.SZ", "2025-01-02 10:00:00")
    df = pd.read_csv(fetcher.cache_file)
    assert len(df) == 1
    assert df.iloc[0]["last_updated_at"] == "2025-01-02 10:00:00"
    
    # 3. Add another entry
    fetcher._update_cache("000002.SZ", "2025-01-02 11:00:00")
    df = pd.read_csv(fetcher.cache_file)
    assert len(df) == 2
    assert "000002.SZ" in df["ts_code"].values
