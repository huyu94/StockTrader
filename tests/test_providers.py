import os
import time
import pytest
import threading
from unittest.mock import MagicMock, patch
import pandas as pd
from src.providers.tushare_provider import TushareProvider

@pytest.fixture
def mock_tushare_env():
    with patch.dict(os.environ, {"TUSHARE_TOKEN": "dummy_token"}):
        yield

@pytest.fixture
def mock_ts_api():
    with patch("tushare.pro_api") as mock_api:
        mock_client = MagicMock()
        mock_api.return_value = mock_client
        yield mock_client

def test_tushare_provider_singleton(mock_tushare_env, mock_ts_api):
    # Reset singleton
    TushareProvider._instance = None
    
    p1 = TushareProvider()
    p2 = TushareProvider()
    assert p1 is p2
    assert p1.pro is not None

def test_tushare_provider_rate_limit(mock_tushare_env, mock_ts_api):
    TushareProvider._instance = None
    provider = TushareProvider()
    provider.RATE_LIMIT_PER_MINUTE = 2
    provider.WINDOW_SECONDS = 0.2
    provider.pro.query.return_value = pd.DataFrame()
    start = time.time()
    provider.query("daily")
    provider.query("daily")
    provider.query("daily")
    elapsed = time.time() - start
    assert elapsed >= 0.18

def test_tushare_provider_query(mock_tushare_env, mock_ts_api):
    TushareProvider._instance = None
    provider = TushareProvider()
    
    expected_df = pd.DataFrame({"ts_code": ["000001.SZ"]})
    provider.pro.query.return_value = expected_df
    
    df = provider.query("daily", ts_code="000001.SZ")
    
    assert df.equals(expected_df)
    provider.pro.query.assert_called_with("daily", fields=None, ts_code="000001.SZ")

def test_tushare_provider_max_concurrency(mock_tushare_env, mock_ts_api):
    TushareProvider._instance = None
    provider = TushareProvider()
    inflight = 0
    max_inflight = 0
    lock = threading.Lock()
    def side_effect(*args, **kwargs):
        nonlocal inflight, max_inflight
        with lock:
            inflight += 1
            if inflight > max_inflight:
                max_inflight = inflight
        time.sleep(0.1)
        with lock:
            inflight -= 1
        return pd.DataFrame()
    provider.pro.query.side_effect = side_effect
    threads = []
    for _ in range(5):
        t = threading.Thread(target=lambda: provider.query("daily"))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    assert max_inflight <= 2
