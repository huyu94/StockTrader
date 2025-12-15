import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

class StockVisualizer:
    def __init__(self, output_dir: str = "visualizations"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def plot_kline_with_bbi(self, df: pd.DataFrame, ts_code: str, save: bool = False) -> None:
        """
        绘制K线图和BBI指标
        :param df: 包含BBI指标的股票数据
        :param ts_code: 股票代码
        :param save: 是否保存图片
        """
        # 创建子图
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, gridspec_kw={'height_ratios': [3, 1]})
        
        # 绘制K线图
        # 由于matplotlib没有直接的K线图，我们使用蜡烛图的简化版本
        df = df.tail(100)  # 只显示最近100天的数据
        
        # 绘制收盘价和BBI
        ax1.plot(df['日期'], df['收盘价'], label='Close Price', color='blue', linewidth=1.5)
        ax1.plot(df['日期'], df['BBI'], label='BBI', color='red', linewidth=2)
        
        ax1.set_title(f'{ts_code} K线图与BBI指标')
        ax1.set_ylabel('Price')
        ax1.legend()
        ax1.grid(True)
        
        # 绘制成交量
        ax2.bar(df['日期'], df['成交量'], color='green', alpha=0.6)
        ax2.set_ylabel('Volume')
        ax2.set_xlabel('Date')
        ax2.grid(True)
        
        plt.tight_layout()
        
        if save:
            file_path = os.path.join(self.output_dir, f"{ts_code}_kline_bbi.png")
            plt.savefig(file_path, dpi=300, bbox_inches='tight')
            print(f"图表已保存到：{file_path}")
        
        plt.show()
    
    def plot_kline_with_macd(self, df: pd.DataFrame, ts_code: str, save: bool = False) -> None:
        """
        绘制K线图和MACD指标
        :param df: 包含MACD指标的股票数据
        :param ts_code: 股票代码
        :param save: 是否保存图片
        """
        # 使用plotly创建交互式图表
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, 
                           vertical_spacing=0.05, 
                           subplot_titles=(f'{ts_code} K线图', '成交量', 'MACD'),
                           row_heights=[0.6, 0.2, 0.2])
        
        df = df.tail(100)  # 只显示最近100天的数据
        
        # 绘制K线图
        fig.add_trace(
            go.Candlestick(x=df['日期'],
                          open=df['开盘价'],
                          high=df['最高价'],
                          low=df['最低价'],
                          close=df['收盘价'],
                          name='K线'),
            row=1, col=1
        )
        
        # 绘制成交量
        fig.add_trace(
            go.Bar(x=df['日期'], y=df['成交量'], name='Volume', marker_color='green', opacity=0.6),
            row=2, col=1
        )
        
        # 绘制MACD
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['MACD'], name='MACD', line=dict(color='blue')),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['DEA'], name='DEA', line=dict(color='orange')),
            row=3, col=1
        )
        
        # MACD柱状图
        fig.add_trace(
            go.Bar(x=df['日期'], y=df['MACD_HIST'], name='MACD_HIST', marker_color=df['MACD_HIST'].apply(lambda x: 'red' if x < 0 else 'green')),
            row=3, col=1
        )
        
        # 更新布局
        fig.update_layout(
            title=f'{ts_code} K线图与MACD指标',
            xaxis_rangeslider_visible=False,
            height=800,
            width=1200,
            showlegend=True
        )
        
        # 更新X轴
        fig.update_xaxes(title_text='Date', row=3, col=1)
        
        # 更新Y轴
        fig.update_yaxes(title_text='Price', row=1, col=1)
        fig.update_yaxes(title_text='Volume', row=2, col=1)
        fig.update_yaxes(title_text='MACD', row=3, col=1)
        
        if save:
            file_path = os.path.join(self.output_dir, f"{ts_code}_kline_macd.html")
            fig.write_html(file_path)
            print(f"图表已保存到：{file_path}")
        
        fig.show()
    
    def plot_rsi(self, df: pd.DataFrame, ts_code: str, save: bool = False) -> None:
        """
        绘制RSI指标
        :param df: 包含RSI指标的股票数据
        :param ts_code: 股票代码
        :param save: 是否保存图片
        """
        df = df.tail(100)  # 只显示最近100天的数据
        
        fig, ax = plt.subplots(figsize=(15, 6))
        
        # 绘制RSI
        ax.plot(df['日期'], df['RSI'], label='RSI', color='purple', linewidth=2)
        
        # 添加超买超卖线
        ax.axhline(y=70, color='red', linestyle='--', alpha=0.7, label='Overbought (70)')
        ax.axhline(y=30, color='green', linestyle='--', alpha=0.7, label='Oversold (30)')
        ax.axhline(y=50, color='gray', linestyle='-', alpha=0.5, label='Midline (50)')
        
        ax.set_title(f'{ts_code} RSI指标')
        ax.set_ylabel('RSI')
        ax.set_xlabel('Date')
        ax.legend()
        ax.grid(True)
        
        plt.tight_layout()
        
        if save:
            file_path = os.path.join(self.output_dir, f"{ts_code}_rsi.png")
            plt.savefig(file_path, dpi=300, bbox_inches='tight')
            print(f"图表已保存到：{file_path}")
        
        plt.show()
    
    def plot_kdj(self, df: pd.DataFrame, ts_code: str, save: bool = False) -> None:
        """
        绘制KDJ指标
        :param df: 包含KDJ指标的股票数据
        :param ts_code: 股票代码
        :param save: 是否保存图片
        """
        df = df.tail(100)  # 只显示最近100天的数据
        
        fig, ax = plt.subplots(figsize=(15, 6))
        
        # 绘制KDJ
        ax.plot(df['日期'], df['K'], label='K', color='blue', linewidth=1.5)
        ax.plot(df['日期'], df['D'], label='D', color='orange', linewidth=1.5)
        ax.plot(df['日期'], df['J'], label='J', color='green', linewidth=1.5)
        
        # 添加超买超卖线
        ax.axhline(y=80, color='red', linestyle='--', alpha=0.7, label='Overbought (80)')
        ax.axhline(y=20, color='green', linestyle='--', alpha=0.7, label='Oversold (20)')
        
        ax.set_title(f'{ts_code} KDJ指标')
        ax.set_ylabel('KDJ')
        ax.set_xlabel('Date')
        ax.legend()
        ax.grid(True)
        
        plt.tight_layout()
        
        if save:
            file_path = os.path.join(self.output_dir, f"{ts_code}_kdj.png")
            plt.savefig(file_path, dpi=300, bbox_inches='tight')
            print(f"图表已保存到：{file_path}")
        
        plt.show()
    
    def plot_all_indicators(self, df: pd.DataFrame, ts_code: str, save: bool = False) -> None:
        """
        绘制所有指标的综合图表
        :param df: 包含所有指标的股票数据
        :param ts_code: 股票代码
        :param save: 是否保存图片
        """
        df = df.tail(100)  # 只显示最近100天的数据
        
        # 使用plotly创建交互式图表
        fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                           vertical_spacing=0.05, 
                           subplot_titles=(f'{ts_code} K线图与BBI', '成交量', 'MACD', 'RSI & KDJ'),
                           row_heights=[0.4, 0.15, 0.2, 0.25])
        
        # 1. 绘制K线图和BBI
        fig.add_trace(
            go.Candlestick(x=df['日期'],
                          open=df['开盘价'],
                          high=df['最高价'],
                          low=df['最低价'],
                          close=df['收盘价'],
                          name='K线'),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['BBI'], name='BBI', line=dict(color='red', width=2)),
            row=1, col=1
        )
        
        # 2. 绘制成交量
        fig.add_trace(
            go.Bar(x=df['日期'], y=df['成交量'], name='Volume', marker_color='green', opacity=0.6),
            row=2, col=1
        )
        
        # 3. 绘制MACD
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['MACD'], name='MACD', line=dict(color='blue')),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['DEA'], name='DEA', line=dict(color='orange')),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Bar(x=df['日期'], y=df['MACD_HIST'], name='MACD_HIST', 
                  marker_color=df['MACD_HIST'].apply(lambda x: 'red' if x < 0 else 'green')),
            row=3, col=1
        )
        
        # 4. 绘制RSI和KDJ
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['RSI'], name='RSI', line=dict(color='purple', yaxis='y1')),
            row=4, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['K'], name='K', line=dict(color='blue', yaxis='y2')),
            row=4, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['D'], name='D', line=dict(color='orange', yaxis='y2')),
            row=4, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df['日期'], y=df['J'], name='J', line=dict(color='green', yaxis='y2')),
            row=4, col=1
        )
        
        # 更新布局
        fig.update_layout(
            title=f'{ts_code} 综合技术指标',
            xaxis_rangeslider_visible=False,
            height=1200,
            width=1400,
            showlegend=True
        )
        
        # 更新X轴
        fig.update_xaxes(title_text='Date', row=4, col=1)
        
        # 更新Y轴
        fig.update_yaxes(title_text='Price', row=1, col=1)
        fig.update_yaxes(title_text='Volume', row=2, col=1)
        fig.update_yaxes(title_text='MACD', row=3, col=1)
        fig.update_yaxes(title_text='RSI', row=4, col=1, side='left', range=[0, 100])
        fig.update_yaxes(title_text='KDJ', row=4, col=1, side='right', range=[0, 100], overlaying='y1')
        
        # 添加RSI超买超卖线
        fig.add_hline(y=70, x0=df['日期'].min(), x1=df['日期'].max(), 
                     line=dict(color='red', dash='dash', opacity=0.7), row=4, col=1)
        fig.add_hline(y=30, x0=df['日期'].min(), x1=df['日期'].max(), 
                     line=dict(color='green', dash='dash', opacity=0.7), row=4, col=1)
        
        # 添加KDJ超买超卖线
        fig.add_hline(y=80, x0=df['日期'].min(), x1=df['日期'].max(), 
                     line=dict(color='red', dash='dash', opacity=0.7), row=4, col=1)
        fig.add_hline(y=20, x0=df['日期'].min(), x1=df['日期'].max(), 
                     line=dict(color='green', dash='dash', opacity=0.7), row=4, col=1)
        
        if save:
            file_path = os.path.join(self.output_dir, f"{ts_code}_all_indicators.html")
            fig.write_html(file_path)
            print(f"图表已保存到：{file_path}")
        
        fig.show()

# 示例用法
if __name__ == "__main__":
    # 这里需要先获取数据并计算指标，然后调用可视化方法
    # 例如：
    # from src.data_fetch.stock_data_fetcher import StockDataFetcher
    # from src.indicators.technical_indicators import TechnicalIndicators
    # 
    # fetcher = StockDataFetcher()
    # df = fetcher.get_daily_k_data('000001.SZ', start_date='20240101', end_date='20241231')
    # 
    # indicators_calculator = TechnicalIndicators()
    # df_with_indicators = indicators_calculator.calculate_all_indicators(df)
    # 
    # visualizer = StockVisualizer()
    # visualizer.plot_all_indicators(df_with_indicators, '000001.SZ', save=True)
    pass