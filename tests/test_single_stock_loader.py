"""
AdjFactorCollector 测试文件

使用 pytest 测试复权因子采集器的单一股票爬取功能
"""

import sys
from pathlib import Path
import pytest
import pandas as pd
import numpy as np
# 添加项目根目录到路径
project_path = Path(__file__).parent.parent
print(project_path)
sys.path.insert(0, str(project_path))

from core.loaders.daily_kline import DailyKlineLoader


# 创建测试数据，将字典包装在列表中以创建单行 DataFrame
df = pd.DataFrame([{
    'ts_code': '920641.BJ', 
    'trade_date': '2015-07-06', 
    'open': 0.01, 
    'high': 0.01, 
    'low': 0.01, 
    'close': 0.01, 
    'change': np.nan, 
    'vol': 10100.0, 
    'amount': 10.1
}])
loader = DailyKlineLoader()

loader.load(df)
