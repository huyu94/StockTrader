import sys
import os
from datetime import datetime, timedelta
from tqdm import tqdm

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetch.stock_data_fetcher import StockDataFetcher
import tushare as ts
import pandas as pd
from src.config import TUSHARE_TOKEN, DATA_PATH

# 初始化Tushare
pro = ts.pro_api(TUSHARE_TOKEN)

def download_trade_calendar(year: int, retry_times: int = 3, delay: float = 1.0) -> None:
    """
    下载指定年份的交易日历（带重试机制）
    :param year: 年份
    :param retry_times: 重试次数
    :param delay: 请求间隔（秒）
    """
    import time
    print(f"=== 开始下载{year}年交易日历 ===")
    
    # 构造开始和结束日期
    start_date = f"{year}0101"
    end_date = f"{year}1231"
    
    for attempt in range(retry_times):
        try:
            # 添加请求间隔
            time.sleep(delay)
            
            # 下载交易日历
            df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields='exchange,cal_date,is_open,pretrade_date')
            
            # 保存到本地
            calendar_path = os.path.join(DATA_PATH, f"trade_calendar_{year}.csv")
            df.to_csv(calendar_path, index=False, encoding='utf-8-sig')
            
            print(f"交易日历已保存到：{calendar_path}")
            print(f"共{len(df)}条记录")
            print(f"其中交易日：{len(df[df['is_open'] == 1])}天")
            print(f"其中非交易日：{len(df[df['is_open'] == 0])}天")
            print(f"=== {year}年交易日历下载完成 ===")
            return
        except Exception as e:
            print(f"下载{year}年交易日历失败（第{attempt+1}/{retry_times}次）：{e}")
            # 如果不是最后一次尝试，增加延迟
            if attempt < retry_times - 1:
                time.sleep(delay * 2)
    
    print(f"=== {year}年交易日历下载失败，已重试{retry_times}次 ===")

def crawl_stock_basic_info(save_local: bool = True) -> pd.DataFrame:
    """
    爬取所有A股股票的基本信息
    :param save_local: 是否保存到本地
    :return: 股票基本信息DataFrame
    """
    print("=== 开始爬取股票基本信息 ===")
    
    try:
        # 获取所有股票基本信息
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market,exchange,curr_type,list_status,delist_date,is_hs')
        
        print(f"共获取{len(df)}只股票的基本信息")
        
        # 保存到本地
        if save_local:
            basic_info_path = os.path.join(DATA_PATH, "stock_basic_info.csv")
            df.to_csv(basic_info_path, index=False, encoding='utf-8-sig')
            print(f"股票基本信息已保存到：{basic_info_path}")
        
        print("=== 股票基本信息爬取完成 ===")
        return df
    except Exception as e:
        print(f"爬取股票基本信息失败：{e}")
        return pd.DataFrame()

def main():
    """
    主函数，爬取A股今年的所有股票数据并下载交易日历
    """
    # 获取当前年份
    current_year = datetime.now().year
    
    print(f"=== 开始爬取{current_year}年A股所有股票数据 ===")
    
    # 初始化数据获取器
    fetcher = StockDataFetcher()
    
    # 爬取股票基本信息
    crawl_stock_basic_info()
    
    # 下载交易日历
    download_trade_calendar(current_year)
    
    # 构造开始和结束日期
    start_date = f"{current_year}0101"
    end_date = datetime.now().strftime("%Y%m%d")
    
    print(f"\n=== 开始爬取{current_year}年至今的A股所有股票数据 ===")
    print(f"开始日期：{start_date}")
    print(f"结束日期：{end_date}")
    
    # 获取所有股票代码
    stock_codes = fetcher.get_all_stock_codes()
    print(f"共{len(stock_codes)}只股票需要爬取")
    
    # 使用批量获取方法，利用pro_daily接口的批量查询功能
    result = fetcher.get_batch_daily_k_data(start_date=start_date, end_date=end_date, save_local=True)
    print(f"\n成功爬取{len(result)}只股票的日线数据")
    
    # 获取复权因子（串行方式，带进度条）
    print(f"\n=== 开始获取复权因子 ===")
    
    # 获取所有股票代码
    stock_codes = list(result.keys()) if result else fetcher.get_all_stock_codes()
    
    # 使用tqdm添加进度条
    for ts_code in tqdm(stock_codes, desc="获取复权因子", unit="只"):
        fetcher.get_adj_factor(ts_code, start_date, end_date, save_local=True)
    
    print(f"\n=== 复权因子获取完成 ===")
    
    print(f"\n=== {current_year}年A股所有股票数据爬取完成 ===")

if __name__ == "__main__":
    main()