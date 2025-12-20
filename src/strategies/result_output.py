#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票策略筛选结果输出系统
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from loguru import logger


class StockResultOutput:
    """
    股票筛选结果输出类，支持多种格式输出
    """
    
    def __init__(self, output_dir: str = "output"):
        """
        初始化结果输出类
        :param output_dir: 输出目录
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
    
    def save_as_json(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        将结果保存为JSON格式
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not filename:
            # 自动生成文件名
            filename = f"stock_filter_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        if not filename.endswith('.json'):
            filename += '.json'
        
        file_path = self.get_output_file_path(filename)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"结果已保存为JSON格式：{file_path}")
            return file_path
        except Exception as e:
            logger.error(f"保存JSON结果失败：{e}")
            raise
    
    def save_as_csv(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        将结果保存为CSV格式
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not filename:
            # 自动生成文件名
            filename = f"stock_filter_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        file_path = self.get_output_file_path(filename)
        
        try:
            # 处理嵌套字典，将KDJ等嵌套结构展开
            flat_result = []
            for item in result:
                flat_item = item.copy()
                # 展开KDJ字典
                if 'kdj' in flat_item and isinstance(flat_item['kdj'], dict):
                    kdj_data = flat_item.pop('kdj')
                    flat_item['KDJ_K'] = kdj_data.get('K', '')
                    flat_item['KDJ_D'] = kdj_data.get('D', '')
                    flat_item['KDJ_J'] = kdj_data.get('J', '')
                flat_result.append(flat_item)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame(flat_result)
            df.to_csv(file_path, index=False, encoding='utf-8')
            
            logger.info(f"结果已保存为CSV格式：{file_path}")
            return file_path
        except Exception as e:
            logger.error(f"保存CSV结果失败：{e}")
            raise
    
    def save_as_excel(self, result: List[Dict[str, Any]], filename: str = None) -> str:
        """
        将结果保存为Excel格式
        :param result: 筛选结果列表
        :param filename: 文件名，若为None则自动生成
        :return: 输出文件路径
        """
        if not filename:
            # 自动生成文件名
            filename = f"stock_filter_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        if not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        file_path = self.get_output_file_path(filename)
        
        try:
            # 处理嵌套字典，将KDJ等嵌套结构展开
            flat_result = []
            for item in result:
                flat_item = item.copy()
                # 展开KDJ字典
                if 'kdj' in flat_item and isinstance(flat_item['kdj'], dict):
                    kdj_data = flat_item.pop('kdj')
                    flat_item['KDJ_K'] = kdj_data.get('K', '')
                    flat_item['KDJ_D'] = kdj_data.get('D', '')
                    flat_item['KDJ_J'] = kdj_data.get('J', '')
                flat_result.append(flat_item)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame(flat_result)
            df.to_excel(file_path, index=False, engine='openpyxl')
            
            logger.info(f"结果已保存为Excel格式：{file_path}")
            return file_path
        except Exception as e:
            logger.error(f"保存Excel结果失败：{e}")
            raise
    
    def print_result(self, result: List[Dict[str, Any]], max_items: int = 10) -> None:
        """
        打印筛选结果
        :param result: 筛选结果列表
        :param max_items: 最大打印数量
        """
        if not result:
            logger.info("没有符合条件的股票")
            return
        
        logger.info(f"共筛选出{len(result)}只符合条件的股票")
        
        # 打印表头
        print("\n" + "="*120)
        print(f"{'股票代码':<12} {'股票名称':<10} {'行业':<15} {'交易所':<8} {'交易日期':<12} {'收盘价':<8} {'20日涨幅(%)':<12} {'KDJ(K)':<8} {'KDJ(D)':<8} {'KDJ(J)':<8} {'RSI':<6} {'BBI':<8}")
        print("="*120)
        
        # 打印结果
        for i, item in enumerate(result[:max_items]):
            print(f"{item['ts_code']:<12} {item['name']:<10} {item['industry']:<15} {item['exchange']:<8} {item['trade_date']:<12} {item['close']:<8} {item['20d_gain']:<12} {item['kdj']['K']:<8} {item['kdj']['D']:<8} {item['kdj']['J']:<8} {item['rsi']:<6} {item['bbi']:<8}")
        
        if len(result) > max_items:
            print(f"\n... 还有{len(result) - max_items}只股票未显示")
        
        print("="*120 + "\n")
    
    def save_result(self, result: List[Dict[str, Any]], formats: List[str] = None, filename_prefix: str = None) -> List[str]:
        """
        保存结果到多种格式
        :param result: 筛选结果列表
        :param formats: 输出格式列表，可选值：['json', 'csv', 'excel']，若为None则保存为所有格式
        :param filename_prefix: 文件名前缀
        :return: 输出文件路径列表
        """
        if not formats:
            formats = ['json', 'csv', 'excel']
        
        file_paths = []
        
        # 生成文件名前缀
        if filename_prefix:
            base_filename = filename_prefix
        else:
            base_filename = f"stock_filter_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        for fmt in formats:
            if fmt.lower() == 'json':
                file_paths.append(self.save_as_json(result, base_filename))
            elif fmt.lower() == 'csv':
                file_paths.append(self.save_as_csv(result, base_filename))
            elif fmt.lower() == 'excel':
                file_paths.append(self.save_as_excel(result, base_filename))
            else:
                logger.warning(f"不支持的输出格式：{fmt}")
        
        return file_paths
    
    def generate_summary(self, result: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        生成筛选结果摘要
        :param result: 筛选结果列表
        :return: 摘要字典
        """
        if not result:
            return {
                "total_stocks": 0,
                "summary": "没有符合条件的股票",
                "detail": ""
            }
        
        # 统计各行业的股票数量
        industry_stats = {}
        for item in result:
            industry = item.get('industry', '未知')
            industry_stats[industry] = industry_stats.get(industry, 0) + 1
        
        # 按股票数量排序
        sorted_industries = sorted(industry_stats.items(), key=lambda x: x[1], reverse=True)
        
        # 生成摘要
        summary = {
            "total_stocks": len(result),
            "date_generated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "industry_distribution": sorted_industries,
            "average_20d_gain": round(sum(item['20d_gain'] for item in result) / len(result), 2),
            "top_5_stocks": result[:5],
            "top_industry": sorted_industries[0][0] if sorted_industries else "未知",
            "top_industry_count": sorted_industries[0][1] if sorted_industries else 0
        }
        
        return summary
    
    def print_summary(self, summary: Dict[str, Any]) -> None:
        """
        打印结果摘要
        :param summary: 摘要字典
        """
        logger.info("\n" + "="*80)
        logger.info("股票筛选结果摘要")
        logger.info("="*80)
        logger.info(f"生成时间：{summary['date_generated']}")
        logger.info(f"符合条件的股票总数：{summary['total_stocks']}")
        logger.info(f"平均20日涨幅：{summary['average_20d_gain']}%")
        logger.info(f"热门行业：{summary['top_industry']}（{summary['top_industry_count']}只）")
        
        if summary['industry_distribution']:
            logger.info("\n行业分布：")
            for industry, count in summary['industry_distribution']:
                logger.info(f"  {industry}: {count}只")
        
        if summary['top_5_stocks']:
            logger.info("\n涨幅前5的股票：")
            for i, stock in enumerate(summary['top_5_stocks']):
                logger.info(f"  {i+1}. {stock['name']}({stock['ts_code']}): {stock['20d_gain']}%")
        
        logger.info("="*80 + "\n")
