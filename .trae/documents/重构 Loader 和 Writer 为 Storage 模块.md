# 重构 Loader 和 Writer 为 Storage 模块 (保留异步并发)

我明白，写入性能非常关键。在合并过程中，我会严格保留原 Writer 中的**异步并发写入逻辑**（使用 `ThreadPoolExecutor`）。

## 核心变更点

新的 `Storage` 类将同时包含原 Loader 的读取能力和 Writer 的并发写入能力。

### 1. `src/storage/daily_kline_storage.py` (合并 `DailyKlineWriter` + `StockLoader`)
- **保留并发**: 移植 `DailyKlineWriter.write_batch` 方法，保持 `ThreadPoolExecutor(max_workers=20)` 的并发写入逻辑，确保写入速度。
- **功能**: 
  - `load(ts_code)`: 读取单只股票数据
  - `write_batch(df)`: 并发写入多只股票数据
  - `write_one(ts_code, df)`: 写入单只 (作为底层方法)

### 2. `src/storage/adj_factor_storage.py` (合并 `AdjFactorWriter` + `AdjFactorLoader`)
- **保留并发**: 移植 `AdjFactorWriter.write_batch` 方法，保持多线程写入。
- **缓存管理**: 统一管理 `adj_factor_cache.csv` 的读取（用于判断更新）和写入（更新时间戳），并加锁保证线程安全。

### 3. 其他 Storage (`BasicInfo`, `Calendar`)
- 合并对应的 Loader 和 Writer，保持现有的单文件写入逻辑（这两类数据量小，通常不需要并发）。

## 执行步骤

1.  **创建 `src/storage` 目录**。
2.  **实现 Storage 类**: 将代码迁移并合并，重点检查并发写入逻辑的完整性。
3.  **更新调用方**: 修改 `Manager` 层（如 `DailyKlineManager`），将 `self.writer` 替换为 `self.storage`，并调用 `self.storage.write_batch`。
4.  **验证**: 运行代码确保并发写入依然生效且文件正确生成。
5.  **清理**: 删除旧的 `loaders` 和 `writers`。
