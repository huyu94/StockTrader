#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票筛选结果可视化模块
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import List, Dict, Any
from loguru import logger

# 设置中文字体，避免中文显示问题
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


class StockResultVisualizer:
    """
    股票筛选结果可视化类
    """
    
    def __init__(self, output_dir: str = "visualization_output"):
        """
        初始化可视化类
        :param output_dir: 可视化结果输出目录
        """
        self.output_dir = output_dir
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
    
    def get_output_file_path(self, filename: str) -> str:
        """
        获取输出文件路径
        :param filename: 文件名
        :return: 完整的输出文件路径
        """
        return os.path.join(self.output_dir, filename)
    
    def plot_industry_distribution(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        绘制行业分布饼图
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not result:
            logger.warning("没有数据可绘制行业分布")
            return None
        
        # 统计各行业的股票数量
        industry_stats = {}
        for item in result:
            industry = item.get('industry', '未知')
            industry_stats[industry] = industry_stats.get(industry, 0) + 1
        
        # 转换为DataFrame
        df = pd.DataFrame(list(industry_stats.items()), columns=['行业', '数量'])
        df = df.sort_values(by='数量', ascending=False)
        
        # 绘制饼图
        plt.figure(figsize=(12, 8))
        plt.pie(df['数量'], labels=df['行业'], autopct='%1.1f%%', startangle=140)
        plt.title('股票行业分布', fontsize=16)
        plt.axis('equal')  # 确保饼图是圆形
        plt.tight_layout()
        
        # 保存图片
        if not filename:
            filename = f"industry_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        if not filename.endswith('.png'):
            filename += '.png'
        
        file_path = self.get_output_file_path(filename)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"行业分布饼图已保存：{file_path}")
        return file_path
    
    def plot_20d_gain_distribution(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        绘制20日涨幅分布直方图
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not result:
            logger.warning("没有数据可绘制20日涨幅分布")
            return None
        
        # 提取20日涨幅数据
        gains = [item['20d_gain'] for item in result]
        
        # 绘制直方图
        plt.figure(figsize=(12, 6))
        sns.histplot(gains, bins=20, kde=True, color='skyblue')
        plt.title('20日涨幅分布', fontsize=16)
        plt.xlabel('20日涨幅(%)', fontsize=12)
        plt.ylabel('股票数量', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # 保存图片
        if not filename:
            filename = f"20d_gain_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        if not filename.endswith('.png'):
            filename += '.png'
        
        file_path = self.get_output_file_path(filename)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"20日涨幅分布直方图已保存：{file_path}")
        return file_path
    
    def plot_kdj_scatter(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        绘制KDJ散点图
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not result:
            logger.warning("没有数据可绘制KDJ散点图")
            return None
        
        # 提取KDJ数据
        k_values = [item['kdj']['K'] for item in result]
        d_values = [item['kdj']['D'] for item in result]
        j_values = [item['kdj']['J'] for item in result]
        gains = [item['20d_gain'] for item in result]
        
        # 绘制散点图
        plt.figure(figsize=(12, 8))
        
        # 创建散点图，使用20日涨幅作为颜色
        scatter = plt.scatter(k_values, d_values, c=gains, cmap='viridis', s=j_values*5, alpha=0.7)
        
        # 添加颜色条
        cbar = plt.colorbar(scatter)
        cbar.set_label('20日涨幅(%)', fontsize=12)
        
        # 添加参考线
        plt.axhline(y=20, color='r', linestyle='--', alpha=0.5, label='KDJ=20参考线')
        plt.axvline(x=20, color='r', linestyle='--', alpha=0.5)
        plt.axhline(y=80, color='r', linestyle='--', alpha=0.5, label='KDJ=80参考线')
        plt.axvline(x=80, color='r', linestyle='--', alpha=0.5)
        
        # 添加对角线
        plt.plot([0, 100], [0, 100], 'k--', alpha=0.5, label='K=D参考线')
        
        plt.title('KDJ散点图', fontsize=16)
        plt.xlabel('K值', fontsize=12)
        plt.ylabel('D值', fontsize=12)
        plt.xlim(0, 100)
        plt.ylim(0, 100)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        # 保存图片
        if not filename:
            filename = f"kdj_scatter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        if not filename.endswith('.png'):
            filename += '.png'
        
        file_path = self.get_output_file_path(filename)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"KDJ散点图已保存：{file_path}")
        return file_path
    
    def plot_top_stocks_gain(self, result: List[Dict[str, Any]], top_n: int = 10, filename: str = None) -> str:
        """
        绘制涨幅前N只股票的柱状图
        :param result: 筛选结果列表
        :param top_n: 前N只股票
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not result:
            logger.warning("没有数据可绘制涨幅前N只股票")
            return None
        
        # 按20日涨幅排序，取前N只
        top_stocks = sorted(result, key=lambda x: x['20d_gain'], reverse=True)[:top_n]
        
        # 提取数据
        stock_names = [f"{item['name']}\n{item['ts_code']}" for item in top_stocks]
        gains = [item['20d_gain'] for item in top_stocks]
        
        # 绘制柱状图
        plt.figure(figsize=(14, 8))
        bars = plt.bar(range(len(top_stocks)), gains, color='skyblue')
        
        # 添加数值标签
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.5, f'{height:.1f}%',
                    ha='center', va='bottom', fontsize=10)
        
        plt.title(f'涨幅前{top_n}只股票', fontsize=16)
        plt.xlabel('股票', fontsize=12)
        plt.ylabel('20日涨幅(%)', fontsize=12)
        plt.xticks(range(len(top_stocks)), stock_names, rotation=45, ha='right')
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        # 保存图片
        if not filename:
            filename = f"top_{top_n}_stocks_gain_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        if not filename.endswith('.png'):
            filename += '.png'
        
        file_path = self.get_output_file_path(filename)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"涨幅前{top_n}只股票柱状图已保存：{file_path}")
        return file_path
    
    def generate_dashboard(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        生成综合仪表盘
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not result:
            logger.warning("没有数据可生成仪表盘")
            return None
        
        # 创建一个大的画布，包含多个子图
        fig = plt.figure(figsize=(20, 15))
        
        # 1. 行业分布饼图（左上角）
        ax1 = fig.add_subplot(2, 2, 1)
        industry_stats = {}
        for item in result:
            industry = item.get('industry', '未知')
            industry_stats[industry] = industry_stats.get(industry, 0) + 1
        df_industry = pd.DataFrame(list(industry_stats.items()), columns=['行业', '数量'])
        df_industry = df_industry.sort_values(by='数量', ascending=False)
        ax1.pie(df_industry['数量'], labels=df_industry['行业'], autopct='%1.1f%%', startangle=140)
        ax1.set_title('股票行业分布', fontsize=14)
        ax1.axis('equal')
        
        # 2. 20日涨幅分布直方图（右上角）
        ax2 = fig.add_subplot(2, 2, 2)
        gains = [item['20d_gain'] for item in result]
        sns.histplot(gains, bins=20, kde=True, color='skyblue', ax=ax2)
        ax2.set_title('20日涨幅分布', fontsize=14)
        ax2.set_xlabel('20日涨幅(%)')
        ax2.set_ylabel('股票数量')
        ax2.grid(True, alpha=0.3)
        
        # 3. KDJ散点图（左下角）
        ax3 = fig.add_subplot(2, 2, 3)
        k_values = [item['kdj']['K'] for item in result]
        d_values = [item['kdj']['D'] for item in result]
        j_values = [item['kdj']['J'] for item in result]
        scatter = ax3.scatter(k_values, d_values, c=gains, cmap='viridis', s=j_values*5, alpha=0.7)
        cbar = fig.colorbar(scatter, ax=ax3)
        cbar.set_label('20日涨幅(%)')
        ax3.axhline(y=20, color='r', linestyle='--', alpha=0.5)
        ax3.axvline(x=20, color='r', linestyle='--', alpha=0.5)
        ax3.axhline(y=80, color='r', linestyle='--', alpha=0.5)
        ax3.axvline(x=80, color='r', linestyle='--', alpha=0.5)
        ax3.plot([0, 100], [0, 100], 'k--', alpha=0.5)
        ax3.set_title('KDJ散点图', fontsize=14)
        ax3.set_xlabel('K值')
        ax3.set_ylabel('D值')
        ax3.set_xlim(0, 100)
        ax3.set_ylim(0, 100)
        ax3.grid(True, alpha=0.3)
        
        # 4. 涨幅前10只股票柱状图（右下角）
        ax4 = fig.add_subplot(2, 2, 4)
        top_stocks = sorted(result, key=lambda x: x['20d_gain'], reverse=True)[:10]
        stock_names = [item['name'] for item in top_stocks]
        top_gains = [item['20d_gain'] for item in top_stocks]
        bars = ax4.bar(range(len(top_stocks)), top_gains, color='skyblue')
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.5, f'{height:.1f}%',
                    ha='center', va='bottom', fontsize=9)
        ax4.set_title('涨幅前10只股票', fontsize=14)
        ax4.set_xlabel('股票')
        ax4.set_ylabel('20日涨幅(%)')
        ax4.set_xticks(range(len(top_stocks)))
        ax4.set_xticklabels(stock_names, rotation=45, ha='right')
        ax4.grid(True, alpha=0.3, axis='y')
        
        # 整体标题
        fig.suptitle('股票筛选结果分析', fontsize=18, y=0.95)
        
        plt.tight_layout()
        
        # 保存图片
        if not filename:
            filename = f"stock_analysis_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        if not filename.endswith('.png'):
            filename += '.png'
        
        file_path = self.get_output_file_path(filename)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"综合仪表盘已保存：{file_path}")
        return file_path
    
    def visualize_result(self, result: List[Dict[str, Any]], filename_prefix: str = None) -> List[str]:
        """
        可视化筛选结果，生成多种图表
        :param result: 筛选结果列表
        :param filename_prefix: 文件名前缀
        :return: 输出文件路径列表
        """
        if not result:
            logger.warning("没有数据可可视化")
            return []
        
        output_files = []
        
        # 1. 行业分布饼图
        output_files.append(self.plot_industry_distribution(result))
        
        # 2. 20日涨幅分布直方图
        output_files.append(self.plot_20d_gain_distribution(result))
        
        # 3. KDJ散点图
        output_files.append(self.plot_kdj_scatter(result))
        
        # 4. 涨幅前10只股票柱状图
        output_files.append(self.plot_top_stocks_gain(result))
        
        # 5. 综合仪表盘
        output_files.append(self.generate_dashboard(result))
        
        # 过滤掉None值
        output_files = [f for f in output_files if f is not None]
        
        return output_files
