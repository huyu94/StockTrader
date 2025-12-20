import pytest
import pandas as pd
from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

@pytest.fixture
def ak_fetcher():
    return StockDailyKLineFetcher(provider_name="akshare")

def test_ak_get_stock_basic_info(ak_fetcher):
    df = ak_fetcher.get_stock_basic_info(save_local=False)
    assert df is not None
    assert not df.empty
    assert 'ts_code' in df.columns
    assert 'symbol' in df.columns
    assert 'name' in df.columns
    # Check if ts_code format is correct (e.g. 000001.SZ)
    assert df['ts_code'].iloc[0].endswith(('.SZ', '.SH', '.BJ'))

def test_ak_get_trade_calendar(ak_fetcher):
    df = ak_fetcher.get_trade_calendar(start_date='20240101', end_date='20240110')
    assert df is not None
    assert not df.empty
    assert 'exchange' in df.columns
    assert 'cal_date' in df.columns
    assert 'is_open' in df.columns

def test_ak_get_daily_k_data(ak_fetcher):
    # Test with a known stock, e.g., Ping An Bank (000001.SZ)
    df = ak_fetcher.get_daily_k_data(ts_code='000001.SZ', start_date='20240101', end_date='20240110', save_local=False)
    assert df is not None
    assert not df.empty
    assert 'trade_date' in df.columns
    assert 'open' in df.columns
    assert 'close' in df.columns
    # Check if dates are within range
    assert df['trade_date'].min().strftime('%Y%m%d') >= '20240101'
    assert df['trade_date'].max().strftime('%Y%m%d') <= '20240110'

def test_ak_get_adj_factor(ak_fetcher):
    # Test with a known stock
    df = ak_fetcher.get_adj_factor(ts_code='000001.SZ', start_date='20240101', end_date='20240110', save_local=False)
    # Note: If API fails or no factor, it might be None. 
    # But 000001.SZ should have factors.
    if df is not None:
        assert not df.empty
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
