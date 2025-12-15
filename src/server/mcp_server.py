from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import pandas as pd
from src.data_fetch.stock_data_fetcher import StockDataFetcher
from src.indicators.technical_indicators import TechnicalIndicators
from src.strategies.base_strategy import (
    BaseStrategy, BBIStrategy, MACDGoldenCrossStrategy,
    RSIStrategy, KDJStrategy, CombinedStrategy, HotPlateStrategy
)

# 初始化应用
app = FastAPI(title="Stock Analysis MCP Server", version="1.0.0")

# 初始化各个模块
fetcher = StockDataFetcher()
indicators_calculator = TechnicalIndicators()

# 策略映射
strategy_map = {
    "bbi": BBIStrategy,
    "macd": MACDGoldenCrossStrategy,
    "rsi": RSIStrategy,
    "kdj": KDJStrategy,
    "combined": CombinedStrategy,
    "hot_plate": HotPlateStrategy
}

# 请求模型
class StockRequest(BaseModel):
    ts_code: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class MultiStockRequest(BaseModel):
    ts_codes: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class StrategyFilterRequest(BaseModel):
    strategy_name: str
    ts_codes: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class IndicatorCalculationRequest(BaseModel):
    ts_code: str
    indicators: List[str]  # 支持的指标："all", "bbi", "macd", "rsi", "kdj"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

# 响应模型
class StockDataResponse(BaseModel):
    ts_code: str
    data: List[Dict]

class StrategyFilterResponse(BaseModel):
    strategy_name: str
    selected_stocks: List[str]

class IndicatorListResponse(BaseModel):
    indicators: List[str]

class StrategyListResponse(BaseModel):
    strategies: List[str]

# 根路径
@app.get("/")
def root():
    return {"message": "Welcome to Stock Analysis MCP Server"}

# 获取支持的技术指标列表
@app.get("/indicators/list", response_model=IndicatorListResponse)
def get_indicators_list():
    """
    获取支持的技术指标列表
    """
    return IndicatorListResponse(indicators=["all", "bbi", "macd", "rsi", "kdj"])

# 获取支持的策略列表
@app.get("/strategies/list", response_model=StrategyListResponse)
def get_strategies_list():
    """
    获取支持的策略列表
    """
    return StrategyListResponse(strategies=list(strategy_map.keys()))

# 获取单只股票数据
@app.post("/stock/data", response_model=StockDataResponse)
def get_stock_data(request: StockRequest):
    """
    获取单只股票的日线K线数据
    """
    try:
        df = fetcher.get_daily_k_data(
            ts_code=request.ts_code,
            start_date=request.start_date,
            end_date=request.end_date,
            save_local=False
        )
        
        # 将DataFrame转换为字典列表
        data = df.to_dict(orient="records")
        
        return StockDataResponse(ts_code=request.ts_code, data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取股票数据失败：{str(e)}")

# 批量获取多只股票数据
@app.post("/stock/multi-data")
def get_multi_stock_data(request: MultiStockRequest):
    """
    批量获取多只股票的日线K线数据
    """
    try:
        result = fetcher.get_multi_stocks_daily_k(
            ts_codes=request.ts_codes,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        # 转换数据格式
        response_data = {}
        for ts_code, df in result.items():
            response_data[ts_code] = df.to_dict(orient="records")
        
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量获取股票数据失败：{str(e)}")

# 计算技术指标
@app.post("/indicators/calculate")
def calculate_indicators(request: IndicatorCalculationRequest):
    """
    计算指定股票的技术指标
    """
    try:
        # 获取原始数据
        df = fetcher.get_daily_k_data(
            ts_code=request.ts_code,
            start_date=request.start_date,
            end_date=request.end_date,
            save_local=False
        )
        
        # 计算指标
        if "all" in request.indicators:
            df_with_indicators = indicators_calculator.calculate_all_indicators(df)
        else:
            df_with_indicators = df.copy()
            for indicator in request.indicators:
                if indicator == "bbi":
                    df_with_indicators = indicators_calculator.calculate_bbi(df_with_indicators)
                elif indicator == "macd":
                    df_with_indicators = indicators_calculator.calculate_macd(df_with_indicators)
                elif indicator == "rsi":
                    df_with_indicators = indicators_calculator.calculate_rsi(df_with_indicators)
                elif indicator == "kdj":
                    df_with_indicators = indicators_calculator.calculate_kdj(df_with_indicators)
        
        return {
            "ts_code": request.ts_code,
            "data": df_with_indicators.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"计算技术指标失败：{str(e)}")

# 使用策略筛选股票
@app.post("/strategies/filter", response_model=StrategyFilterResponse)
def filter_stocks_by_strategy(request: StrategyFilterRequest):
    """
    使用指定策略筛选股票
    """
    try:
        # 检查策略是否支持
        if request.strategy_name not in strategy_map:
            raise HTTPException(status_code=400, detail=f"不支持的策略：{request.strategy_name}")
        
        # 获取多只股票数据
        stocks_data = fetcher.get_multi_stocks_daily_k(
            ts_codes=request.ts_codes,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        # 初始化策略
        strategy = strategy_map[request.strategy_name]()
        
        # 筛选股票
        selected_stocks = strategy.filter_stocks(stocks_data)
        
        return StrategyFilterResponse(
            strategy_name=request.strategy_name,
            selected_stocks=selected_stocks
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"策略筛选失败：{str(e)}")

# 运行服务器
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)