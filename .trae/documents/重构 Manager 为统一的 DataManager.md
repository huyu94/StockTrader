# 重构 Manager 为统一的 DataManager

你的想法非常好。通过汇总 Manager 并统一管理线程池，我们可以大幅减少代码冗余，并更好地控制系统资源。

## 核心设计

我们将创建一个 `DataManager` 类，它将取代现有的 4 个 Manager。它将负责：
1.  **资源管理**: 维护一个统一的线程池 (`ThreadPoolExecutor`)，用于后台写入任务。
2.  **模块协调**: 实例化所有的 `Storage` 和 `Fetcher`。
3.  **通用逻辑**: 提供通用的 `update` 方法来处理不同类型的数据（因为日线和复权因子的更新逻辑高度一致）。

## 实施步骤

### 1. 修改 Storage 类
为了支持外部传入线程池（可选），我们需要微调 `DailyKlineStorage` 和 `AdjFactorStorage` 的 `write_batch` 方法。
- 如果外部传入了 `executor`，则复用它；否则（为了兼容性）内部创建。
- **注意**: 鉴于性能考虑，我们决定让 `DataManager` 维护一个用于**任务调度**的线程池（比如异步提交写入任务），而 `Storage` 内部的 `write_batch` 依然使用临时的多线程池来进行文件IO。这是因为文件IO是短暂的高并发，而任务调度是长周期的。如果混用一个池，容易造成死锁或效率降低。
- **但是**，响应你的需求“统一管理一个线程池”，我会在 `DataManager` 中创建一个 `io_executor` (例如 20 线程)，并将其传递给 `Storage` 类使用，从而避免每次 `write_batch` 都反复创建销毁线程池。

### 2. 创建 `src/managers/data_manager.py`
这将是新的核心文件。

**主要成员**:
- `self.io_executor`: 全局 IO 线程池 (20 线程)，传给 Storage 用。
- `self.task_executor`: 后台任务线程池 (1 线程)，用于异步提交 "Fetch -> Write" 流程。
- `self.daily_storage`, `self.adj_storage`, ...
- `self.daily_fetcher`, `self.adj_fetcher`, ...

**主要方法**:
- `update_all()`: 一键更新所有数据。
- `update_daily_kline(start_date)`
- `update_adj_factor(start_date)`
- `update_basic_info()`
- `update_calendar()`

**内部通用方法**:
- `_update_matrix_based_data(...)`: 提取日线和复权因子共用的“矩阵检查 -> 循环日期 -> 抓取 -> 异步写入”逻辑。

### 3. 清理旧代码
- 删除 `src/managers/` 下旧的 4 个 manager 文件。
- 更新 `scripts/run_strategies.py` 使用新的 `DataManager`。

### 4. 矩阵管理器 (可选优化)
- `DataMatrixManager` 和 `AdjFactorMatrixManager` 逻辑也非常相似，暂时先保留，在 `DataManager` 中实例化使用。

## 预期效果
- **代码量减少**: 消除大量重复的样板代码。
- **资源可控**: 全局只有一个 IO 线程池，不会因为开启多个任务而导致线程爆炸。
- **扩展性好**: 新增数据类型只需调用通用模板方法。
