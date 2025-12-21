import os
import shutil
import json
import pandas as pd
from unittest.mock import MagicMock
from src.fetchers.calendar_fetcher import CalendarFetcher
from project_var import CACHE_DIR

def test_calendar_fetcher_save(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    cache_dir = tmp_path / "cache"
    os.environ["DATA_PATH"] = str(data_dir)
    
    # Mock CACHE_DIR in project_var for this test if possible, 
    # but since it is imported, we might need to patch the instance attribute instead.
    
    mock_provider = MagicMock()
    df_mock = pd.DataFrame({
        "exchange": ["SSE", "SSE", "SSE"],
        "cal_date": ["20250102", "20250103", "20250104"],
        "is_open": [1, 0, 1]
    })
    df_expected = df_mock[df_mock["is_open"] == 1].sort_values("cal_date").reset_index(drop=True)
    mock_provider.query.return_value = df_expected
    
    fetcher = CalendarFetcher(provider_name="tushare", provider=mock_provider)
    # Override cache_dir for test isolation
    fetcher.cache_dir = str(cache_dir)
    os.makedirs(fetcher.cache_dir, exist_ok=True)
    
    out = fetcher.fetch("20250101", "20250131", "SSE", save_local=True)
    
    assert out.equals(df_expected)
    files = os.listdir(str(data_dir))
    assert any(f.startswith("trade_calendar_SSE_20250101_20250131") and f.endswith(".csv") for f in files)
    
    # Verify cache file creation
    cache_file = os.path.join(str(cache_dir), "calendar_SSE_update.json")
    assert os.path.exists(cache_file)
    with open(cache_file, "r", encoding="utf-8") as f:
        cache_data = json.load(f)
        assert cache_data["exchange"] == "SSE"
        assert "last_updated_at" in cache_data
        
    # Clean up
    if os.path.exists(str(data_dir)):
        shutil.rmtree(str(data_dir))
    if os.path.exists(str(cache_dir)):
        shutil.rmtree(str(cache_dir))
