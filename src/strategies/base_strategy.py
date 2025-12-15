import pandas as pd
from abc import ABC, abstractmethod
from src.indicators.technical_indicators import TechnicalIndicators

class BaseStrategy(ABC):
    """
    策略基类，所有自定义策略都应继承此类
    """
    
    def __init__(self):
        self.indicators_calculator = TechnicalIndicators()
    
    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备数据，计算所需的技术指标
        :param df: 原始股票数据
        :return: 添加了技术指标的数据
        """
        return self.indicators_calculator.calculate_all_indicators(df)
    
    @abstractmethod
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号判断
        :param df: 包含技术指标的股票数据
        :return: 是否应该买入
        """
        pass
    
    @abstractmethod
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号判断
        :param df: 包含技术指标的股票数据
        :return: 是否应该卖出
        """
        pass
    
    def filter_stocks(self, stocks_data: dict) -> list:
        """
        筛选符合策略的股票
        :param stocks_data: 股票代码为键，DataFrame为值的字典
        :return: 符合策略的股票代码列表
        """
        result = []
        
        for ts_code, df in stocks_data.items():
            try:
                # 准备数据
                df_with_indicators = self.prepare_data(df)
                
                # 检查买入信号
                if self.should_buy(df_with_indicators):
                    result.append(ts_code)
            except Exception as e:
                print(f"筛选{ts_code}时出错：{e}")
        
        return result


class BBIStrategy(BaseStrategy):
    """
    BBI策略示例：当收盘价上穿BBI线时买入，下穿BBI线时卖出
    """
    
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号：收盘价上穿BBI线
        """
        if len(df) < 2:
            return False
        
        # 检查前一天和当天的情况
        prev_close = df['收盘价'].iloc[-2]
        prev_bbi = df['BBI'].iloc[-2]
        curr_close = df['收盘价'].iloc[-1]
        curr_bbi = df['BBI'].iloc[-1]
        
        # 前一天收盘价低于BBI，当天收盘价高于BBI
        return (prev_close < prev_bbi) and (curr_close > curr_bbi)
    
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号：收盘价下穿BBI线
        """
        if len(df) < 2:
            return False
        
        prev_close = df['收盘价'].iloc[-2]
        prev_bbi = df['BBI'].iloc[-2]
        curr_close = df['收盘价'].iloc[-1]
        curr_bbi = df['BBI'].iloc[-1]
        
        return (prev_close > prev_bbi) and (curr_close < curr_bbi)


class MACDGoldenCrossStrategy(BaseStrategy):
    """
    MACD金叉策略：当MACD线上穿DEA线时买入，死叉时卖出
    """
    
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号：MACD金叉（MACD线上穿DEA线）
        """
        if len(df) < 2:
            return False
        
        prev_macd = df['MACD'].iloc[-2]
        prev_dea = df['DEA'].iloc[-2]
        curr_macd = df['MACD'].iloc[-1]
        curr_dea = df['DEA'].iloc[-1]
        
        return (prev_macd < prev_dea) and (curr_macd > curr_dea)
    
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号：MACD死叉（MACD线下穿DEA线）
        """
        if len(df) < 2:
            return False
        
        prev_macd = df['MACD'].iloc[-2]
        prev_dea = df['DEA'].iloc[-2]
        curr_macd = df['MACD'].iloc[-1]
        curr_dea = df['DEA'].iloc[-1]
        
        return (prev_macd > prev_dea) and (curr_macd < curr_dea)


class RSIStrategy(BaseStrategy):
    """
    RSI策略示例：RSI低于30时买入，高于70时卖出
    """
    
    def __init__(self, oversold: int = 30, overbought: int = 70):
        super().__init__()
        self.oversold = oversold
        self.overbought = overbought
    
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号：RSI低于超卖线
        """
        if len(df) == 0:
            return False
        
        return df['RSI'].iloc[-1] < self.oversold
    
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号：RSI高于超买线
        """
        if len(df) == 0:
            return False
        
        return df['RSI'].iloc[-1] > self.overbought


class KDJStrategy(BaseStrategy):
    """
    KDJ策略示例：K线上穿D线且J线低于20时买入，K线下穿D线且J线高于80时卖出
    """
    
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号：K线上穿D线且J线低于20
        """
        if len(df) < 2:
            return False
        
        # K线上穿D线
        prev_k = df['K'].iloc[-2]
        prev_d = df['D'].iloc[-2]
        curr_k = df['K'].iloc[-1]
        curr_d = df['D'].iloc[-1]
        golden_cross = (prev_k < prev_d) and (curr_k > curr_d)
        
        # J线低于20
        curr_j = df['J'].iloc[-1]
        j_oversold = curr_j < 20
        
        return golden_cross and j_oversold
    
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号：K线下穿D线且J线高于80
        """
        if len(df) < 2:
            return False
        
        # K线下穿D线
        prev_k = df['K'].iloc[-2]
        prev_d = df['D'].iloc[-2]
        curr_k = df['K'].iloc[-1]
        curr_d = df['D'].iloc[-1]
        death_cross = (prev_k > prev_d) and (curr_k < curr_d)
        
        # J线高于80
        curr_j = df['J'].iloc[-1]
        j_overbought = curr_j > 80
        
        return death_cross and j_overbought


class CombinedStrategy(BaseStrategy):
    """
    组合策略示例：结合MACD金叉和RSI超卖信号
    """
    
    def __init__(self):
        super().__init__()
        self.macd_strategy = MACDGoldenCrossStrategy()
        self.rsi_strategy = RSIStrategy(oversold=30, overbought=70)
    
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号：同时满足MACD金叉和RSI超卖
        """
        return self.macd_strategy.should_buy(df) and self.rsi_strategy.should_buy(df)
    
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号：满足MACD死叉或RSI超买
        """
        return self.macd_strategy.should_sell(df) or self.rsi_strategy.should_sell(df)

class HotPlateStrategy(BaseStrategy):
    """
    热门板块策略：筛选符合以下条件的股票
    1. 属于热门板块（科技、核聚变、商业航天等）
    2. 前20日内最低点到最高点，涨幅大于20%
    3. 最后一天收盘，KDJ指标小于10
    """
    
    def __init__(self):
        super().__init__()
        self.hot_industries = ['科技', '电子', '半导体', '计算机', '人工智能', '核聚变', '航天', '卫星', '新能源', '光伏', '风电']
        from src.data_fetch.stock_data_fetcher import StockDataFetcher
        self.fetcher = StockDataFetcher()
        # 加载所有股票的基本信息
        try:
            self.stock_basic = self.fetcher.get_stock_basic_info()
            # 合并所有交易所的数据
            self.stock_basic = pd.concat([
                self.stock_basic,
                self.fetcher.get_stock_basic_info('SZSE'),
                self.fetcher.get_stock_basic_info('BSE')
            ])
        except Exception as e:
            print(f"加载股票基本信息失败：{e}")
            self.stock_basic = pd.DataFrame()
    
    def is_hot_industry(self, ts_code: str) -> bool:
        """
        判断股票是否属于热门板块
        :param ts_code: 股票代码
        :return: 是否属于热门板块
        """
        if self.stock_basic.empty:
            return False
        
        stock_info = self.stock_basic[self.stock_basic['ts_code'] == ts_code]
        if stock_info.empty:
            return False
        
        industry = stock_info['industry'].iloc[0]
        # 检查行业是否在热门板块列表中
        for hot_industry in self.hot_industries:
            if hot_industry in industry:
                return True
        
        return False
    
    def check_20d_gain(self, df: pd.DataFrame) -> bool:
        """
        检查前20日内最低点到最高点的涨幅是否大于20%
        :param df: 股票数据
        :return: 涨幅是否大于20%
        """
        if len(df) < 20:
            return False
        
        # 获取前20日的数据（包括当天）
        recent_20d = df.tail(20)
        
        # 计算最低点和最高点
        low = recent_20d['最低价'].min()
        high = recent_20d['最高价'].max()
        
        # 计算涨幅
        gain = (high - low) / low * 100
        
        return gain > 20
    
    def check_kdj_less_than_10(self, df: pd.DataFrame) -> bool:
        """
        检查最后一天的KDJ指标是否小于10
        :param df: 包含KDJ指标的股票数据
        :return: KDJ是否小于10
        """
        if len(df) == 0:
            return False
        
        # 检查K、D、J值是否都小于10
        last_k = df['K'].iloc[-1]
        last_d = df['D'].iloc[-1]
        last_j = df['J'].iloc[-1]
        
        return last_k < 10 and last_d < 10 and last_j < 10
    
    def should_buy(self, df: pd.DataFrame) -> bool:
        """
        买入信号：同时满足所有条件
        """
        # 注意：这里的ts_code需要从df中获取，但当前df没有包含ts_code信息
        # 我们需要修改策略筛选逻辑，将ts_code传递给should_buy方法
        # 为了兼容现有代码，我们先返回False，后面再修改
        return False
    
    def should_sell(self, df: pd.DataFrame) -> bool:
        """
        卖出信号：暂时不实现
        """
        return False
    
    def filter_stocks(self, stocks_data: dict) -> list:
        """
        筛选符合策略的股票
        :param stocks_data: 股票代码为键，DataFrame为值的字典
        :return: 符合策略的股票代码列表
        """
        result = []
        
        for ts_code, df in stocks_data.items():
            try:
                # 1. 检查是否属于热门板块
                if not self.is_hot_industry(ts_code):
                    continue
                
                # 2. 准备数据，计算技术指标
                df_with_indicators = self.prepare_data(df)
                
                # 3. 检查前20日涨幅是否大于20%
                if not self.check_20d_gain(df_with_indicators):
                    continue
                
                # 4. 检查最后一天KDJ是否小于10
                if not self.check_kdj_less_than_10(df_with_indicators):
                    continue
                
                # 所有条件都满足
                result.append(ts_code)
                print(f"{ts_code} 符合热门板块策略")
                
            except Exception as e:
                print(f"筛选{ts_code}时出错：{e}")
        
        return result


# 示例用法
if __name__ == "__main__":
    # 这里需要先获取股票数据，然后使用策略筛选
    # 例如：
    # from src.data_fetch.stock_data_fetcher import StockDataFetcher
    # fetcher = StockDataFetcher()
    # stocks = ['000001.SZ', '600000.SH', '000858.SZ']
    # stocks_data = fetcher.get_multi_stocks_daily_k(stocks, start_date='20240101', end_date='20241231')
    # 
    # # 使用BBI策略筛选
    # bbi_strategy = BBIStrategy()
    # selected_stocks = bbi_strategy.filter_stocks(stocks_data)
    # print(f"符合BBI策略的股票：{selected_stocks}")
    # 
    # # 使用MACD策略筛选
    # macd_strategy = MACDGoldenCrossStrategy()
    # selected_stocks = macd_strategy.filter_stocks(stocks_data)
    # print(f"符合MACD策略的股票：{selected_stocks}")
    # 
    # # 使用热门板块策略筛选
    # hot_strategy = HotPlateStrategy()
    # selected_stocks = hot_strategy.filter_stocks(stocks_data)
    # print(f"符合热门板块策略的股票：{selected_stocks}")
    pass