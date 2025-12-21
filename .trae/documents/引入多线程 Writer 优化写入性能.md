我将通过引入 `Writer` 类并将写入操作并行化来优化 DailyKline 的性能。

### 计划步骤

1.  **创建 Writers 模块**
    *   创建 `src/writers` 目录。
    *   实现 `src/writers/daily_kline_writer.py`：
        *   使用 `ThreadPoolExecutor` 实现多线程写入。
        *   将大的 DataFrame 按 `ts_code` 分组，并发地将每只股票的数据追加写入到对应的 CSV 文件中。
        *   实现“读取旧数据 -> 合并去重 -> 写入新数据”的原子逻辑。

2.  **重构 DailyKline 模块**
    *   **DailyKlineFetcher**: 移除 `save_daily_data_to_stock_files` 方法，使其专注于拉取数据并返回 DataFrame。
    *   **DailyKlineManager**:
        *   引入 `DailyKlineWriter`。
        *   在获取到 Tushare 的全市场日线数据后，调用 Writer 进行并发写入。

3.  **后续扩展（可选）**
    *   虽然 `AdjFactor` 模块目前已经是并发拉取+写入（在 Manager 层并发），但为了架构统一，后续也可以为其添加 `AdjFactorWriter`，实现职责分离（Fetcher 只拉取，Writer 只写入）。本次优先解决性能瓶颈最明显的 `DailyKline`。

### 预期效果
*   将 5000+ 个文件的串行写入操作转变为并发写入。
*   显著减少 `DailyKline` 脚本在“保存数据”阶段的耗时。
