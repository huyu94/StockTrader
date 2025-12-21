import os
import pytest
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

def test_tushare_provider_query(mock_tushare_env, mock_ts_api):
    TushareProvider._instance = None
    provider = TushareProvider()
    
    expected_df = pd.DataFrame({"ts_code": ["000001.SZ"]})
    provider.pro.query.return_value = expected_df
    
    df = provider.query("daily", ts_code="000001.SZ")
    
    assert df.equals(expected_df)
    provider.pro.query.assert_called_with("daily", fields=None, ts_code="000001.SZ")
