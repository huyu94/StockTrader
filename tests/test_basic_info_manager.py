import os
import shutil
import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock
from src.managers.basic_info_manager import BasicInfoManager

@pytest.fixture
def mock_env(tmp_path):
    data_dir = tmp_path / "data"
    cache_dir = tmp_path / "cache"
    os.environ["DATA_PATH"] = str(data_dir)
    return data_dir, cache_dir

def test_basic_info_manager_all_stocks(mock_env, monkeypatch):
    data_dir, cache_dir = mock_env
    
    # Mock data with 'exchange' column
    mock_df = pd.DataFrame({
        "ts_code": ["000001.SZ", "600000.SH", "830000.BJ"],
        "symbol": ["000001", "600000", "830000"],
        "name": ["平安银行", "浦发银行", "北交所股票"],
        "market": ["主板", "主板", "北交所"],
        "exchange": ["SZSE", "SSE", "BSE"]
    })
    
    with pytest.MonkeyPatch.context() as m:
        manager = BasicInfoManager()
        # Redirect directories
        manager.loader.data_dir = str(data_dir)
        manager.loader.cache_dir = str(cache_dir)
        manager.loader.cache_file = os.path.join(str(cache_dir), "basic_info_update.json")
        
        manager.fetcher.data_dir = str(data_dir)
        manager.fetcher.cache_dir = str(cache_dir)
        
        os.makedirs(manager.loader.data_dir, exist_ok=True)
        os.makedirs(manager.loader.cache_dir, exist_ok=True)
        
        # Mock fetcher.fetch
        manager.fetcher.fetch = MagicMock(return_value=mock_df)
        
        # 1. Get all stocks (trigger fetch)
        df = manager.basic_info
        assert len(df) == 3
        manager.fetcher.fetch.assert_called_once()
        
        # 2. Filter by market
        main_board = manager.get_stocks_by_market(["主板"])
        assert len(main_board) == 2
        assert "830000.BJ" not in main_board["ts_code"].values
        
        bse = manager.get_stocks_by_market(["北交所"])
        assert len(bse) == 1
        assert bse.iloc[0]["ts_code"] == "830000.BJ"
        
        # 3. Filter by exchange
        sse = manager.get_stocks_by_exchange(["SSE"])
        assert len(sse) == 1
        assert sse.iloc[0]["ts_code"] == "600000.SH"
        
        szse_bse = manager.get_stocks_by_exchange(["SZSE", "BSE"])
        assert len(szse_bse) == 2
        assert "600000.SH" not in szse_bse["ts_code"].values
