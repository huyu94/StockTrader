import os
import pandas as pd
from unittest.mock import MagicMock
from src.fetchers.adj_factor_fetcher import AdjFactorFetcher

def test_adj_factor_fetcher_one(tmp_path):
    data_dir = tmp_path / "data"
    os.environ["DATA_PATH"] = str(data_dir)
    mock_provider = MagicMock()
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ"],
        "trade_date": ["20250102", "20250103"],
        "adj_factor": [1.1, 1.2]
    })
    mock_provider.query.return_value = df
    f = AdjFactorFetcher(provider_name="tushare", provider=mock_provider)
    out = f.fetch_one("000001.SZ", "20200101", "20250131", save_local=True)
    assert out.equals(df.sort_values("trade_date").reset_index(drop=True))
    path = os.path.join(str(data_dir), "adj_factor", "000001.SZ.csv")
    assert os.path.exists(path)
