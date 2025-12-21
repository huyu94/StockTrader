我将同时重构 `DailyKline` 和 `AdjFactor` 模块，引入各自的 `Writer` 类来实现并行写入优化。

### 核心优化思路
*   **DailyKline**: 网络请求一次拉取全量数据（单线程），Writer 负责将大数据拆分并**并发写入**到 5000+ 个文件中。
*   **AdjFactor**: 网络请求必须串行（受限 IP），Manager 负责收集数据，Writer 负责将收集到的数据**并发写入**磁盘。这将实现“网络串行 + IO 并行”的最佳效率。

### 详细计划

1.  **创建 Writers 模块**
    *   创建 `src/writers` 目录。
    *   **实现 `DailyKlineWriter`**:
        *   方法 `write_batch(df)`: 接收包含多只股票的 DF，按代码分组，使用线程池并发追加写入文件。
    *   **实现 `AdjFactorWriter`**:
        *   方法 `write_one(ts_code, df)`: 写入单只股票（含缓存更新逻辑）。
        *   方法 `write_batch(results_dict)`: 接收 `{ts_code: df}` 字典，使用线程池并发调用 `write_one`。

2.  **重构 DailyKline 模块**
    *   **Fetcher**: 移除 `save_daily_data_to_stock_files`，只返回 DataFrame。
    *   **Manager**: 在获取到数据后，调用 `DailyKlineWriter.write_batch` 进行并发写入。

3.  **重构 AdjFactor 模块**
    *   **Fetcher**: 移除 `fetch_one` 中的写入和缓存更新逻辑，只返回 DataFrame。
    *   **Manager**:
        *   `batch_get_adj_factors` 保持串行拉取以规避 IP 限制。
        *   改为“生产者-消费者”模式或“分批处理”模式：串行拉取数据暂存内存，每积累一定数量（如 50 个）或结束后，调用 `AdjFactorWriter.write_batch` 进行并发写入。

4.  **清理与验证**
    *   确保所有原有逻辑（如去重、缓存更新）都正确迁移到 Writer 中。
    *   运行脚本验证并发写入的稳定性和速度。
