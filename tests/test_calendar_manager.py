import os
import shutil
import json
import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.managers.calendar_manager import CalendarManager

@pytest.fixture
def mock_env(tmp_path):
    data_dir = tmp_path / "data"
    cache_dir = tmp_path / "cache"
    os.environ["DATA_PATH"] = str(data_dir)
    return data_dir, cache_dir

def test_manager_auto_fetch(mock_env, monkeypatch):
    data_dir, cache_dir = mock_env
    
    # Setup mock fetcher to return dummy data
    mock_df = pd.DataFrame({"cal_date": ["20250101"], "is_open": [1]})
    
    # Patch fetcher query method
    with pytest.MonkeyPatch.context() as m:
        manager = CalendarManager()
        # Redirect directories for isolation
        manager.loader.data_dir = str(data_dir)
        manager.loader.cache_dir = str(cache_dir)
        manager.fetcher.data_dir = str(data_dir)
        manager.fetcher.cache_dir = str(cache_dir)
        os.makedirs(str(data_dir), exist_ok=True)
        os.makedirs(str(cache_dir), exist_ok=True)

        # Mock fetcher.fetch
        manager.fetcher.fetch = MagicMock(return_value=mock_df)
        
        # 1. First call: no local data -> should trigger fetch
        df = manager.sse_calendar
        assert df.equals(mock_df)
        manager.fetcher.fetch.assert_called_once()
        
        # Reset mock
        manager.fetcher.fetch.reset_mock()
        
        # 2. Second call: data exists but cache missing logic
        # Manually write files to simulate "fetcher did its job"
        csv_path = os.path.join(str(data_dir), "SSE_trade_calendar.csv")
        mock_df.to_csv(csv_path, index=False)
        
        cache_path = os.path.join(str(cache_dir), "calendar_SSE_update.json")
        from datetime import datetime
        with open(cache_path, "w") as f:
            json.dump({"exchange": "SSE", "last_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f)
            
        # Re-instantiate manager or clear cached_property if possible. 
        if "sse_calendar" in manager.__dict__:
            del manager.__dict__["sse_calendar"]
            
        # Now call again -> should NOT trigger fetch
        # Note: loader.load reads csv, where cal_date might be read as int/str.
        # Our loader forces cal_date to str.
        df2 = manager.sse_calendar
        manager.fetcher.fetch.assert_not_called()
        assert df2["cal_date"].iloc[0] == "20250101"

def test_manager_merged_calendar_outer_join(mock_env):
    data_dir, cache_dir = mock_env
    manager = CalendarManager()
    manager.loader.data_dir = str(data_dir)
    manager.loader.cache_dir = str(cache_dir)
    manager.fetcher.data_dir = str(data_dir)
    manager.fetcher.cache_dir = str(cache_dir)
    os.makedirs(str(data_dir), exist_ok=True)
    os.makedirs(str(cache_dir), exist_ok=True)
    
    # Case:
    # 20250101: SSE open, SZSE missing -> Outer join: SSE=True, SZSE=False
    # 20250102: Both open -> Outer join: SSE=True, SZSE=True
    # 20250103: SSE missing, SZSE open -> Outer join: SSE=False, SZSE=True
    
    df_sse = pd.DataFrame({"cal_date": ["20250101", "20250102"], "is_open": [1, 1]})
    df_szse = pd.DataFrame({"cal_date": ["20250102", "20250103"], "is_open": [1, 1]})
    
    manager.fetcher.fetch = MagicMock(side_effect=lambda exchange, **kwargs: df_sse if exchange=="SSE" else df_szse)
    
    combined = manager.calendar
    
    # Expected Index: 20250101, 20250102, 20250103
    assert len(combined) == 3
    assert list(combined.columns) == ["SSE", "SZSE"]
    assert combined.index[0] == "20250101"
    assert combined.index[2] == "20250103"
    
    # Check values
    # 01: SSE=True, SZSE=False (filled)
    assert combined.loc["20250101", "SSE"] == True
    assert combined.loc["20250101", "SZSE"] == False
    
    # 02: Both True
    assert combined.loc["20250102", "SSE"] == True
    assert combined.loc["20250102", "SZSE"] == True
    
    # 03: SSE=False (filled), SZSE=True
    assert combined.loc["20250103", "SSE"] == False
    assert combined.loc["20250103", "SZSE"] == True
