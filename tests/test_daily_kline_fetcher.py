import os
import pandas as pd
from unittest.mock import MagicMock
from src.fetchers.daily_kline_fetcher import DailyKlineFetcher

def test_daily_kline_fetcher_one(tmp_path):
    data_dir = tmp_path / "data"
    os.environ["DATA_PATH"] = str(data_dir)
    mock_provider = MagicMock()
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ"],
        "trade_date": ["20250102", "20250103"],
        "open": [10, 11],
        "close": [12, 13]
    })
    mock_provider.query.return_value = df
    f = DailyKlineFetcher(provider_name="tushare", provider=mock_provider)
    out = f.fetch_one("000001.SZ", "20250101", "20250131", save_local=True)
    assert out.equals(df.sort_values("trade_date").reset_index(drop=True))
    path = os.path.join(str(data_dir), "daily", "000001.SZ.csv")
    assert os.path.exists(path)

def test_daily_kline_fetcher_by_date(tmp_path):
    data_dir = tmp_path / "data"
    os.environ["DATA_PATH"] = str(data_dir)
    mock_provider = MagicMock()
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20250102", "20250102"],
        "open": [10, 20],
        "close": [12, 22]
    })
    mock_provider.query.return_value = df
    f = DailyKlineFetcher(provider_name="tushare", provider=mock_provider)
    written = f.fetch_by_date("20250102", codes=None, save_local=True)
    assert set(written) == {"000001.SZ", "000002.SZ"}
    path = os.path.join(str(data_dir), "daily_by_date", "20250102.csv")
    assert os.path.exists(path)
