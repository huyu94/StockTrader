#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指标数据API接口
"""

from fastapi import APIRouter, Query, HTTPException, Body
from typing import List, Optional
import pandas as pd
from loguru import logger

from src.indicators.indicator_calculator import IndicatorCalculator
from src.data_fetch.stock_data_fetcher import StockDailyKLineFetcher

# 创建路由
router = APIRouter()

# 创建指标计算器实例
indicator_calculator = IndicatorCalculator()

# 创建数据获取器实例
stock_fetcher = StockDailyKLineFetcher()


@router.get("/{ts_code}")
async def get_stock_indicators(
    ts_code: str,
    indicators: Optional[List[str]] = Query(None, description="要获取的指标列表，如：BBI,MACD,RSI,KDJ"),
    start_date: Optional[str] = Query(None, description="开始日期，格式：YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式：YYYYMMDD"),
    force_recalculate: Optional[bool] = Query(False, description="是否强制重新计算指标")
):
    """
    获取单只股票的指标数据
    """
    try:
        logger.info(f"获取{ts_code}的指标数据，指标：{indicators}，日期范围：{start_date}-{end_date}")
        
        # 计算指标数据
        df = indicator_calculator.calculate_indicators_for_single_stock(ts_code, indicators, force_recalculate)
        
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"未找到{ts_code}的指标数据")
        
        # 处理日期范围筛选
        if start_date:
            start_date_dt = pd.to_datetime(start_date, format='%Y%m%d')
            df = df[df['trade_date'] >= start_date_dt]
        
        if end_date:
            end_date_dt = pd.to_datetime(end_date, format='%Y%m%d')
            df = df[df['trade_date'] <= end_date_dt]
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"{ts_code}在指定日期范围内没有指标数据")
        
        # 转换日期格式为字符串
        df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
        
        return {
            "ts_code": ts_code,
            "data": df.to_dict(orient="records"),
            "count": len(df),
            "indicators": list(set(df.columns) - {'trade_date', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额'})
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取{ts_code}指标数据失败：{e}")
        raise HTTPException(status_code=500, detail=f"获取指标数据失败：{str(e)}")


@router.post("/calculate")
async def calculate_stock_indicators(
    ts_code: str = Body(..., description="股票代码"),
    indicators: Optional[List[str]] = Body(None, description="要计算的指标列表，如：BBI,MACD,RSI,KDJ"),
    data: Optional[dict] = Body(None, description="股票原始数据，可选，默认使用本地存储数据")
):
    """
    实时计算单只股票的指标
    """
    try:
        logger.info(f"实时计算{ts_code}的指标：{indicators}")
        
        # 如果提供了数据，使用提供的数据进行计算
        if data:
            # 将数据转换为DataFrame
            df = pd.DataFrame(data)
            
            # 确保必要的列存在
            required_columns = ['trade_date', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额']
            if not all(col in df.columns for col in required_columns):
                raise HTTPException(status_code=400, detail=f"提供的数据缺少必要列，需要：{required_columns}")
            
            # 转换日期列为datetime格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 计算指标
            from src.indicators.technical_indicators import TechnicalIndicators
            ti = TechnicalIndicators()
            
            if indicators is None or len(indicators) == 0:
                # 计算所有指标
                result_df = ti.calculate_all_indicators(df)
            else:
                # 只计算指定的指标
                result_df = df.copy()
                for indicator in indicators:
                    if indicator.upper() == 'BBI':
                        result_df = ti.calculate_bbi(result_df)
                    elif indicator.upper() == 'MACD':
                        result_df = ti.calculate_macd(result_df)
                    elif indicator.upper() == 'RSI':
                        result_df = ti.calculate_rsi(result_df)
                    elif indicator.upper() == 'KDJ':
                        result_df = ti.calculate_kdj(result_df)
                    else:
                        raise HTTPException(status_code=400, detail=f"未知指标：{indicator}")
        else:
            # 使用本地数据计算指标
            result_df = indicator_calculator.calculate_indicators_for_single_stock(ts_code, indicators, True)
        
        if result_df is None or result_df.empty:
            raise HTTPException(status_code=404, detail=f"计算{ts_code}指标数据失败")
        
        # 转换日期格式为字符串
        result_df['trade_date'] = result_df['trade_date'].dt.strftime('%Y-%m-%d')
        
        return {
            "ts_code": ts_code,
            "data": result_df.to_dict(orient="records"),
            "count": len(result_df),
            "indicators": list(set(result_df.columns) - {'trade_date', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额'})
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"实时计算{ts_code}指标失败：{e}")
        raise HTTPException(status_code=500, detail=f"实时计算指标失败：{str(e)}")


@router.post("/batch")
async def get_batch_stock_indicators(
    ts_codes: List[str] = Body(..., description="股票代码列表"),
    indicators: Optional[List[str]] = Body(None, description="要获取的指标列表，如：BBI,MACD,RSI,KDJ"),
    force_recalculate: Optional[bool] = Body(False, description="是否强制重新计算指标")
):
    """
    批量获取多只股票的指标数据
    """
    try:
        logger.info(f"批量获取{len(ts_codes)}只股票的指标数据，指标：{indicators}")
        
        # 批量计算指标数据
        result_dict = indicator_calculator.calculate_indicators_for_multiple_stocks(ts_codes, indicators, force_recalculate)
        
        if not result_dict:
            raise HTTPException(status_code=404, detail="未找到任何股票的指标数据")
        
        # 转换结果格式
        result = {}
        for ts_code, df in result_dict.items():
            # 转换日期格式为字符串
            df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
            result[ts_code] = {
                "data": df.to_dict(orient="records"),
                "count": len(df),
                "indicators": list(set(df.columns) - {'trade_date', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额'})
            }
        
        return {
            "total": len(result),
            "stocks": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"批量获取指标数据失败：{e}")
        raise HTTPException(status_code=500, detail=f"批量获取指标数据失败：{str(e)}")


@router.get("/stock/basic")
async def get_stock_basic_info(
    exchange: Optional[str] = Query(None, description="交易所代码，可选值：SSE（上交所）、SZSE（深交所）、BSE（北交所）")
):
    """
    获取股票基本信息
    """
    try:
        logger.info(f"获取股票基本信息，交易所：{exchange}")
        
        # 获取股票基本信息
        if exchange:
            df = stock_fetcher.get_stock_basic_info(exchange, False)
        else:
            # 获取所有交易所的股票基本信息
            df = pd.concat([
                stock_fetcher.get_stock_basic_info('SSE', False),
                stock_fetcher.get_stock_basic_info('SZSE', False),
                stock_fetcher.get_stock_basic_info('BSE', False)
            ])
        
        return {
            "total": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        logger.error(f"获取股票基本信息失败：{e}")
        raise HTTPException(status_code=500, detail=f"获取股票基本信息失败：{str(e)}")
