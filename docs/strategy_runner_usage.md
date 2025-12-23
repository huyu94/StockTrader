# StrategyRunner 使用指南

## 概述

`StrategyRunner` 是一个统一的策略运行器，提供了简洁的接口来运行各种交易策略。

## 功能特性

1. **单股票运行**：检查指定股票在指定日期是否符合策略
2. **批量运行**：批量检查所有股票，筛选符合条件的股票池
3. **可选并发支持**：支持多线程并发运行（需要测试验证）
4. **自动保存结果**：自动保存结果到 CSV 和 JSON 文件
5. **进度显示**：使用 tqdm 显示运行进度

## SQLite 并发说明

### 理论支持

SQLite 在 WAL（Write-Ahead Logging）模式下支持：
- ✅ **多个读连接并发读取**：多个线程可以同时读取数据库
- ❌ **并发写入**：不支持并发写入，会锁数据库

### 实际考虑

策略运行主要是**读取操作**，理论上可以并发：
- 每个线程创建独立的 `Manager` 和 `Storage` 实例
- 每个实例有独立的数据库连接
- WAL 模式已启用（在 `SQLiteBaseStorage` 中）

### 建议

- **默认使用串行模式**（`use_concurrent=False`）：确保稳定性
- **如需启用并发**：
  1. 先在小规模数据上测试（如 100 只股票）
  2. 观察是否有数据库锁定错误
  3. 如果稳定，可以逐步增加并发线程数
  4. 建议并发线程数：2-4 个（过多可能反而变慢）

## 使用方法

### 1. 单股票运行

```python
from src.runner import StrategyRunner
from src.strategies.kdj_strategy import KDJStrategy

# 创建运行器
runner = StrategyRunner()

# 运行策略
result = runner.run_single(
    strategy_class=KDJStrategy,
    ts_code="300642.SZ",
    target_date="20250929",
    start_date="20250201",
    end_date="20251222",
    strategy_kwargs={
        "kdj_period": 9,
        "vol_period": 20,
        "j_threshold": 5.0
    }
)

# 查看结果
if result["success"]:
    print(f"信号: {result['signal']}")
    print(f"解释: {result['explanation']['reason']}")
else:
    print(f"错误: {result['error']}")
```

### 2. 批量运行（串行模式）

```python
from src.runner import StrategyRunner
from src.strategies.kdj_strategy import KDJStrategy

runner = StrategyRunner()

# 批量运行（默认串行）
stock_pool = runner.run_batch(
    strategy_class=KDJStrategy,
    target_date="20251222",
    start_date="20250201",
    end_date="20251222",
    strategy_kwargs={
        "kdj_period": 9,
        "vol_period": 20,
        "j_threshold": 5.0
    },
    use_concurrent=False,  # 串行模式
    save_results=True,
    output_filename="stock_pool"
)

print(f"找到 {len(stock_pool)} 只符合条件的股票")
```

### 3. 批量运行（并发模式 - 需测试）

```python
from src.runner import StrategyRunner
from src.strategies.kdj_strategy import KDJStrategy

runner = StrategyRunner()

# 批量运行（并发模式）
stock_pool = runner.run_batch(
    strategy_class=KDJStrategy,
    target_date="20251222",
    start_date="20250201",
    end_date="20251222",
    strategy_kwargs={
        "kdj_period": 9,
        "vol_period": 20,
        "j_threshold": 5.0
    },
    use_concurrent=True,  # 启用并发
    max_workers=4,  # 4个并发线程
    save_results=True,
    output_filename="stock_pool"
)
```

### 4. 指定股票列表运行

```python
runner = StrategyRunner()

# 只检查指定的股票
ts_codes = ["300642.SZ", "000001.SZ", "000002.SZ"]

stock_pool = runner.run_batch(
    strategy_class=KDJStrategy,
    target_date="20251222",
    start_date="20250201",
    end_date="20251222",
    ts_codes=ts_codes,  # 指定股票列表
    use_concurrent=False
)
```

## 参数说明

### `run_single()` 方法

- `strategy_class`: 策略类（如 `KDJStrategy`）
- `ts_code`: 股票代码
- `target_date`: 目标检查日期（YYYYMMDD格式）
- `start_date`: 开始日期（YYYYMMDD格式）
- `end_date`: 结束日期（YYYYMMDD格式）
- `strategy_kwargs`: 策略初始化参数（字典）

### `run_batch()` 方法

- `strategy_class`: 策略类
- `target_date`: 目标检查日期
- `start_date`: 开始日期
- `end_date`: 结束日期
- `strategy_kwargs`: 策略初始化参数
- `ts_codes`: 股票代码列表（可选，默认使用所有股票）
- `use_concurrent`: 是否使用并发（默认 False）
- `max_workers`: 并发线程数（默认 4，仅在并发模式下有效）
- `save_results`: 是否保存结果（默认 True）
- `output_filename`: 输出文件名（默认 "stock_pool"）

## 返回结果

### 单股票运行返回

```python
{
    "success": bool,           # 是否成功执行
    "ts_code": str,            # 股票代码
    "stock_name": str,         # 股票名称
    "target_date": str,        # 检查日期
    "signal": str,             # 交易信号（"买入"/"观望"）
    "explanation": dict,       # 策略解释信息
    "error": str               # 错误信息（如果有）
}
```

### 批量运行返回

返回 `pd.DataFrame`，包含以下列：
- `ts_code`: 股票代码
- `name`: 股票名称
- `signal`: 交易信号
- `target_date`: 检查日期

## 输出文件

批量运行会自动保存结果到：
- `output/stock_pool.csv`: CSV 格式
- `output/stock_pool.json`: JSON 格式（中文正常显示）

## 性能对比

### 串行模式
- ✅ 稳定可靠
- ✅ 资源占用低
- ⏱️ 速度：约 1-2 只股票/秒（取决于数据量）

### 并发模式（理论）
- ⚡ 可能更快（需要测试验证）
- ⚠️ 需要更多内存（每个线程独立实例）
- ⚠️ 可能遇到数据库锁定问题

## 注意事项

1. **并发模式需要测试**：虽然理论上支持，但建议先在小规模数据上测试
2. **内存占用**：并发模式下每个线程创建独立的 Manager 实例，内存占用会增加
3. **数据库锁定**：如果遇到 "database is locked" 错误，建议使用串行模式
4. **结果保存**：结果会自动保存到 `output/` 目录

## 完整示例

参考 `main.py` 文件，其中包含了完整的使用示例。

