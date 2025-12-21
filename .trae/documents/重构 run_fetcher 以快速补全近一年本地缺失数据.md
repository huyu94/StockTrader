## 目标
- 先计算“交易日缺失表格”（近一年：行=交易日，列=股票代码，值=是否缺失），按行优先补齐。
- 当某个交易日缺失股票数超过阈值（默认 1000），优先用 `pro.daily(trade_date=...)` 一次性抓该日所有股票，再对剩余缺口做单股拉取。
- 限流下沉到 `TushareProvider`（同 IP 2 连接、每分钟 500 次），整体抓取更快更稳。

## 关键改动
- Provider：新增“按交易日全量抓取”接口
  - 在 `src/data/providers/tushare_provider.py` 实现 `get_daily_all(trade_date: str) -> pd.DataFrame`，内部调用 `pro.daily(trade_date=...)`；统一列到当前日线标准（包含 `ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `vol`）。
  - 所有 Provider 方法前置限流：进程级 `Semaphore(2)` + 滑动窗口 500 次/分钟，失败重试也计入。
- 缺失矩阵构建器
  - 新增 `src/data/missing/missing_matrix.py` 定义 `MissingMatrixBuilder`：
    - 读取近一年交易日集合（`src/data/calendars/calendar_fetcher.py:17/24`），
    - 遍历所有代码（`src/data/fetchers/base_info_fetcher.py:35`），只读取各自 CSV 的近一年 `trade_date` 列，计算缺失集合，累计到“交易日 → 缺失代码集合”的 Map，输出两个结果：
      - `date_missing_counts: dict[date, int]`
      - `date_missing_codes: dict[date, set[str]]`
    - 支持增量：读取后可缓存到 `cache/missing_matrix_{date_range}.json`，后续续跑直接加载。
- Fetcher：批量/单股双路径
  - 在 `StockDailyKLineFetcher` 中新增：
    - `fetch_by_date(trade_date: str, codes: set[str])`：调用 Provider `get_daily_all(...)`，对返回 DF 仅写入 `codes` 中缺失的股票；按 `ts_code` 分组，逐文件 append/merge 去重后落盘。
    - `detect_missing_dates(ts_code, end_date, window_days=365)`：用于小缺口（不足阈值）时的单股抓取；保持现有 `_infer_start_date:29` 与 `fetch_batch:61` 逻辑但只覆盖缺口。
- 入口 `run_fetcher` 改造（`scripts/run_fetcher.py:19`）
  - 流程：
    1. 获取全部代码并确保近一年交易日缓存；
    2. 构建缺失矩阵（或加载缓存）；
    3. 将交易日按 `date_missing_counts` 降序排序：
       - 若 `missing_count >= threshold(默认1000)`：执行 `fetch_by_date(trade_date, date_missing_codes[trade_date])`；
       - 否则：将该日缺失转为单股缺口任务，聚合到 `fetch_batch`；
    4. 落盘后更新进度缓存与缺失矩阵（减少重复处理），循环直至近一年补齐。
  - 参数：`--threshold 1000`、`--workers <CPU并发>`、`--only-missing`（默认 true）、`--provider tushare`；保持 `uv run` 入口。

## 算法与性能
- 缺失矩阵构建优化：
  - 仅读近一年 `trade_date` 列（`usecols=['trade_date']`），降低 IO；
  - 多进程/线程读取本地 CSV，CPU 并发高、网络并发始终由 Provider 控制为 2。
- 批量路径：大缺口日优先，单次 `pro.daily` 覆盖上千代码，显著减少 API 次数；随后单股路径只处理余量缺口。
- 落盘：按 `ts_code` 分组写入，去重合并保证幂等。

## 限流与稳健
- Provider 内部：`Semaphore(2)` 控连接；滑动窗口 500/min 拒绝或等待发起；统一重试 `tenacity`（固定间隔或指数退避）。
- run_fetcher：CPU 并发可高（如 8-16），但网络入口不突破 Provider 限流；出现封禁/异常记录到日志并降级处理（跳过该日或回退单股）。

## 测试与验证
- 单元：
  - 缺失矩阵生成正确性（小样本），性能测试（大量 CSV 快速统计）；
  - `get_daily_all(...)` 列映射与限流计数；
  - 批量路径落盘后每个代码的该日数据存在（幂等）。
- 集成：
  - 近一年数据在阈值策略下补齐，用 `uv run pytest` 运行；
  - 实测 `uv run scripts/run_fetcher.py --provider tushare --workers 12 --threshold 1000`。

## 清理与文档
- 统一到新入口，删除/修复旧脚本与不一致接口；
- README 增加“缺失矩阵、批量路径与参数说明”；
- `.trae/rules/project_rules.md` 的“限流机制下沉到 Tushare provider”要求将被满足。