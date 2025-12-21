## 目标
- 取消 run_fetcher.py 内部并发；由 Fetcher 类提供批量拉取接口并在内部处理并发与限流。
- 批量接口统一 start_date 与 end_date，参数一次性应用到所有股票；避免脚本层复杂度。

## 实现方案
- 在 `src/data/fetchers/stock_fetcher.py` 新增方法：
  - `fetch_multi(ts_codes: list[str], start_date: str, end_date: str, save_local: bool = True, workers: int | None = None)`
  - 行为：
    - 校验并规范日期（缺省为最近一年）
    - 根据 Provider 类型决定并发：
      - tushare → 并发强制 1（遵守单 IP 并发=1，速率=450/min 的共享限流）
      - akshare → 使用传入 `workers` 或默认值（如 8）
    - 线程池遍历 ts_codes：
      - 调用 `_call_with_limit(self.provider.get_daily_k_data, ...)` 获取日线
      - `save_to_local_data` 落盘去重
      - `get_adj_factor`（同样限流）
    - tqdm 进度条与错误日志（不抛异常阻断）

## 脚本调整
- `scripts/run_fetcher.py`：
  - 保留前置：基础信息拉取（SSE+SZSE）、日历 ensure、缺失评估
  - 统一拉取区间：使用脚本已有的 `start_date` 与 `end_date`（最近一年）
  - 取消脚本内线程池；改为：`fetcher.fetch_multi(need, start_date, end_date, save_local=True)`

## 限流与稳定性
- 已有共享限流器 `tushare_limiter()`（450/min，concurrency=1），确保全进程内所有 Tushare 请求总并发与速率限制统一生效。
- Provider 内重试等待已提升至 5 秒以适配 IP 并发限制回应。

## 验证
- 运行 `uv run python scripts/run_fetcher.py --provider tushare --limit 50` 验证无“最大数量为2个”错误、进度正常推进。
- 运行 akshare 场景可提升 `workers` 参数，观察吞吐与正确落盘。

## 兼容性
- 现有脚本参数不变（`--provider --limit --workers`），只是脚本内部不再自行并发。
- Loader/CalendarFetcher 逻辑保持不变；Fetcher 只承担拉取与落盘职责。