# IndicatorCalculator 使用指南

## 概述

`IndicatorCalculator` 是一个技术指标计算器类，提供了常用的股票技术指标计算方法。

## 支持的指标

### 1. 移动平均线 (MA)
- 方法：`calculate_ma(df, periods=[5, 10, 20, 60])`
- 返回列：`ma5`, `ma10`, `ma20`, `ma60`

### 2. KDJ 指标
- 方法：`calculate_kdj(df, period=9)`
- 返回列：`kdj_k`, `kdj_d`, `kdj_j`

### 3. MACD 指标
- 方法：`calculate_macd(df, fast_period=12, slow_period=26, signal_period=9)`
- 返回列：`macd_dif`, `macd_dea`, `macd_hist`

### 4. RSI 指标
- 方法：`calculate_rsi(df, period=14)`
- 返回列：`rsi14`

### 5. BBI 指标
- 方法：`calculate_bbi(df)`
- 返回列：`bbi`

### 6. BOLL 指标（布林带）
- 方法：`calculate_boll(df, period=20, num_std=2.0)`
- 返回列：`boll_upper`, `boll_middle`, `boll_lower`

### 7. 成交量指标
- 方法：`calculate_volume_indicators(df, period=20)`
- 返回列：`vol_ma`, `vol_ratio`, `vol_max`

### 8. ATR 指标（平均真实波幅）
- 方法：`calculate_atr(df, period=14)`
- 返回列：`atr`

## 使用方法

### 方式1：一次性计算所有指标

```python
from src.utils.indicator_calculator import IndicatorCalculator

calculator = IndicatorCalculator()

# 一次性计算所有常用指标
df = calculator.calculate_all(
    df,
    ma_periods=[5, 10, 20, 60],
    kdj_period=9,
    macd_fast=12,
    macd_slow=26,
    macd_signal=9,
    rsi_period=14,
    boll_period=20,
    boll_std=2.0
)
```

### 方式2：单独计算某个指标

```python
from src.utils.indicator_calculator import IndicatorCalculator

calculator = IndicatorCalculator()

# 计算移动平均线
df = calculator.calculate_ma(df, periods=[5, 10, 20, 60])

# 计算KDJ
df = calculator.calculate_kdj(df, period=9)

# 计算MACD
df = calculator.calculate_macd(df, fast_period=12, slow_period=26, signal_period=9)

# 计算RSI
df = calculator.calculate_rsi(df, period=14)

# 计算BBI
df = calculator.calculate_bbi(df)

# 计算BOLL
df = calculator.calculate_boll(df, period=20, num_std=2.0)

# 计算成交量指标
df = calculator.calculate_volume_indicators(df, period=20)

# 计算ATR
df = calculator.calculate_atr(df, period=14)
```

### 方式3：在策略中使用

```python
from src.strategies.base_strategy import BaseStrategy

class MyStrategy(BaseStrategy):
    def _preprocess(self, ts_code: str, df: pd.DataFrame) -> pd.DataFrame:
        # 使用基类提供的指标计算器
        calculator = self.indicator_calculator
        
        # 计算所需指标
        df = calculator.calculate_ma(df, periods=[5, 10, 20])
        df = calculator.calculate_kdj(df, period=9)
        df = calculator.calculate_macd(df)
        
        return df
    
    def _check_stock(self, ts_code: str, df: pd.DataFrame) -> bool:
        latest = df.iloc[-1]
        
        # 使用计算好的指标进行判断
        if latest['close'] > latest['ma5'] and latest['kdj_j'] < 20:
            return True
        
        return False
```

## 完整示例

参考 `main.py` 文件，其中展示了完整的使用流程：

```python
from src.manager import Manager
from src.utils.indicator_calculator import IndicatorCalculator

def main():
    # 初始化
    manager = Manager(provider_name="tushare")
    calculator = IndicatorCalculator()
    
    # 加载数据
    df = manager.load_kline_data_from_sql("300642.SZ", "20250101", "20251222")
    
    # 计算所有指标
    df = calculator.calculate_all(df)
    
    # 查看结果
    print(df[['trade_date', 'close', 'ma5', 'kdj_k', 'macd_dif']].tail())
```

## 注意事项

1. **数据要求**：DataFrame 必须包含 `high`, `low`, `close`, `open` 列
2. **数据清洗**：`calculate_all()` 会自动清理 NaN 值
3. **数据量要求**：某些指标需要足够的历史数据（如MA60需要至少60条数据）
4. **不修改原数据**：所有方法都返回新的 DataFrame，不会修改原始数据

## KDJ 指标特别说明

KDJ 指标在计算时可能出现 NaN 的情况：

1. **数据量不足**：如果数据少于周期数（默认9），会返回 NaN
2. **前期数据不足**：rolling 窗口期内的数据会是 NaN

已经在 `IndicatorCalculator` 中做了优化处理：
- 自动检测数据量
- 自动填充初始值为 50
- 处理 NaN 传播问题

