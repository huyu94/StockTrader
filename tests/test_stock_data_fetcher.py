import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import pytest

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDataFetcher
from src.config import DATA_PATH

@pytest.fixture(scope="class")
def fetcher():
    """创建StockDataFetcher实例，所有测试共享"""
    return StockDataFetcher()

@pytest.fixture(scope="class")
def test_params():
    """测试参数"""
    return {
        "test_ts_code": "000001.SZ",
        "test_start_date": "20251201",
        "test_end_date": "20251210"
    }

class TestStockDataFetcher:
    """StockDataFetcher类的pytest单元测试"""
    
    def test_init(self, fetcher):
        """测试初始化方法"""
        assert isinstance(fetcher, StockDataFetcher)
        assert hasattr(fetcher, 'stock_data_path')
        assert hasattr(fetcher, 'adj_factor_path')
        assert os.path.exists(fetcher.stock_data_path)
        assert os.path.exists(fetcher.adj_factor_path)
    
    def test_get_stock_basic_info(self, fetcher):
        """测试获取股票基本信息"""
        # 测试获取上交所股票
        sse_df = fetcher.get_stock_basic_info('SSE')
        assert isinstance(sse_df, pd.DataFrame)
        assert 'ts_code' in sse_df.columns
        assert 'name' in sse_df.columns
        
        # 测试获取深交所股票
        szse_df = fetcher.get_stock_basic_info('SZSE')
        assert isinstance(szse_df, pd.DataFrame)
        
        # 测试获取北交所股票
        bse_df = fetcher.get_stock_basic_info('BSE')
        assert isinstance(bse_df, pd.DataFrame)
    
    def test_get_all_stock_codes(self, fetcher):
        """测试获取所有A股股票代码"""
        stock_codes = fetcher.get_all_stock_codes()
        assert isinstance(stock_codes, list)
        assert len(stock_codes) > 0
        # 验证股票代码格式
        for code in stock_codes[:5]:  # 只验证前5个
            assert code.endswith('.SZ') or code.endswith('.SH') or code.endswith('.BJ')
    
    def test_get_adj_factor(self, fetcher, test_params):
        """测试获取股票复权因子"""
        df = fetcher.get_adj_factor(
            test_params["test_ts_code"], 
            start_date=test_params["test_start_date"], 
            end_date=test_params["test_end_date"], 
            save_local=False
        )
        assert isinstance(df, pd.DataFrame)
        assert 'trade_date' in df.columns
        assert 'adj_factor' in df.columns
        assert not df.empty
    
    def test_get_daily_k_data(self, fetcher, test_params):
        """测试获取单只股票日线数据"""
        df = fetcher.get_daily_k_data(
            test_params["test_ts_code"], 
            start_date=test_params["test_start_date"], 
            end_date=test_params["test_end_date"], 
            save_local=False
        )
        assert isinstance(df, pd.DataFrame)
        assert 'trade_date' in df.columns
        assert 'open' in df.columns
        assert 'close' in df.columns
        assert not df.empty
    
    def test_get_batch_daily_k_data(self, fetcher, test_params):
        """测试批量获取股票日线数据"""
        # 测试获取单个股票
        result1 = fetcher.get_batch_daily_k_data(
            ts_code=test_params["test_ts_code"],
            start_date=test_params["test_start_date"],
            end_date=test_params["test_end_date"],
            save_local=False
        )
        assert isinstance(result1, dict)
        assert test_params["test_ts_code"] in result1
        
        # 测试获取多个股票
        result2 = fetcher.get_batch_daily_k_data(
            ts_code=f"{test_params['test_ts_code']},600000.SH",
            start_date=test_params["test_start_date"],
            end_date=test_params["test_end_date"],
            save_local=False
        )
        assert isinstance(result2, dict)
        assert len(result2) >= 2
    
    def test_detect_missing_dates(self, fetcher, test_params):
        """测试检测缺失的交易日"""
        # 首先获取一些数据用于测试
        df = fetcher.get_daily_k_data(
            test_params["test_ts_code"], 
            start_date=test_params["test_start_date"], 
            end_date=test_params["test_end_date"], 
            save_local=False
        )
        
        # 测试检测缺失日期
        missing_dates = fetcher.detect_missing_dates(
            start_date=test_params["test_start_date"],
            end_date=test_params["test_end_date"],
            df=df
        )
        
        assert isinstance(missing_dates, pd.DatetimeIndex)
    
    def test_generate_holiday_list(self, fetcher):
        """测试生成A股节假日列表"""
        # 生成当前年份的节假日列表
        fetcher.generate_holiday_list()
        
        # 生成指定年份的节假日列表
        current_year = datetime.now().year
        fetcher.generate_holiday_list(current_year)
        
        # 验证文件是否生成
        holiday_path = os.path.join(DATA_PATH, "a股节假日.csv")
        assert os.path.exists(holiday_path)
        
        # 验证文件内容
        df_holidays = pd.read_csv(holiday_path)
        assert isinstance(df_holidays, pd.DataFrame)
        assert 'holiday_date' in df_holidays.columns
    
    def test_load_local_data(self, fetcher, test_params):
        """测试从本地加载股票数据"""
        # 首先保存一些数据到本地
        fetcher.get_daily_k_data(
            test_params["test_ts_code"], 
            start_date=test_params["test_start_date"], 
            end_date=test_params["test_end_date"], 
            save_local=True
        )
        
        # 然后从本地加载
        df = fetcher.load_local_data(test_params["test_ts_code"])
        assert isinstance(df, pd.DataFrame)
        assert 'trade_date' in df.columns
        assert not df.empty
    
    def test_update_stock_data(self, fetcher, test_params):
        """测试更新股票数据"""
        # 测试更新单只股票数据
        # 注意：这是一个集成测试，会实际调用API
        fetcher.update_stock_data(test_params["test_ts_code"])
        
        # 验证数据是否更新
        df = fetcher.load_local_data(test_params["test_ts_code"])
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
    
    def test_get_multi_stocks_daily_k(self, fetcher, test_params):
        """测试批量获取多只股票的日线数据（带重试和限流）"""
        # 测试获取少量股票
        ts_codes = [test_params["test_ts_code"], "600000.SH"]
        result = fetcher.get_multi_stocks_daily_k(
            ts_codes, 
            start_date=test_params["test_start_date"], 
            end_date=test_params["test_end_date"], 
            max_workers=2
        )
        
        assert isinstance(result, dict)
        assert len(result) == len(ts_codes)
        for ts_code in ts_codes:
            assert ts_code in result
            assert isinstance(result[ts_code], pd.DataFrame)
