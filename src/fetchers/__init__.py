"""统一数据获取模块"""
import os
from typing import Optional
import pandas as pd
from loguru import logger
from src.providers import TushareProvider
import dotenv

dotenv.load_dotenv()

# 单例provider实例，确保所有API调用共享同一个实例
PROVIDER = TushareProvider()

def fetch_basic_info(list_status: str = "L", 
                     exchange: str = None,
                     market: str = None,
                     is_hs: str = None,
                     fields: Optional[str] = None) -> pd.DataFrame:
    """
    获取股票基本信息