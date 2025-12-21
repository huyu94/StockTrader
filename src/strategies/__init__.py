"""
策略模块

提供股票交易策略的基类和实现。
所有策略都继承自 BaseStrategy，实现 preprocess 和 check_stock 方法。
"""

from .base_strategy import BaseStrategy
from .example_strategy import SimpleMAStrategy
from .kdj_strategy import KDJStrategy

__all__ = ['BaseStrategy', 'SimpleMAStrategy', 'KDJStrategy']

