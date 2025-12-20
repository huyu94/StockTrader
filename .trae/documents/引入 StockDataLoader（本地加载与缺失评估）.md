## 目标
- 解耦本地数据加载与缺失评估（Loader）与远端数据获取（Fetcher）。
- 当 Loader 发现本地交易日历缓存缺失或覆盖不足时，使用专门的 CalendarFetcher 获取并写入缓存，不再依赖 StockDailyKLineFetcher，进一步职责清晰化。

## 模块与职责
### 1) CalendarCache（只读/写缓存）
- 文件：`src/data_fetch/calendar_cache.py`
- 接口：
  - `load(exchange) -> DataFrame`：读取 `cache/trade_calendar_{exchange}.csv`，不存在返回空。
  - `save(exchange, df)`：原子写入缓存文件。
- 说明：完全不做网络，纯缓存读写。

### 2) CalendarFetcher（只负责日历获取）
- 文件：`src/data_fetch/calendar_fetcher.py`
- 依赖：Provider（Tushare/AkShare）
- 接口：
  - `fetch(exchange, start_date, end_date) -> DataFrame`：从数据源拉取区间日历。
  - `ensure(exchange, start_date, end_date) -> DataFrame`：
    - 读取缓存覆盖范围（使用 CalendarCache.load）；
    - 对不足区间分块增量拉取（年/月粒度），合并去重排序；
    - 写回缓存（使用 CalendarCache.save）；
    - 返回合并后的完整日历。
- 说明：日历的远端获取与缓存合并由该类独立负责，不再混入股票数据抓取逻辑。

### 3) StockDataLoader（不做网络）
- 文件：`src/data_fetch/stock_data_loader.py`
- 依赖：`CalendarCache.load`，`project_var`
- 接口：
  - `load(ts_code) -> DataFrame | None`：读取 `DATA_DIR/stock_data/{ts_code}.csv` 并标准化日期列。
  - `check_missing(ts_code, start_date, end_date, exchange='SSE') -> MissingInfo`：
    - 通过 `CalendarCache.load(exchange)` 获取日历；若为空或覆盖不足：
      - 调用 `CalendarFetcher.ensure(exchange, start_date, end_date)` 以更新缓存，再读一次缓存；
    - 对比本地 `trade_date` 与开放日，返回 `is_need_fetch, missing_dates, local_coverage, reason`。
  - `check_missing_multi(ts_codes, start_date, end_date, exchange='SSE', max_workers=16) -> DataFrame`：并发评估缺失数（仅本地 + 缓存），必要时先通过 CalendarFetcher 预热一次目标区间。
- 数据结构：
  - `MissingInfo`（dataclass）：`is_need_fetch: bool`、`missing_dates: List[str]`、`local_coverage: Tuple[str,str]`、`reason: str`。

### 4) StockDailyKLineFetcher（只负责股票数据获取）
- 文件：`src/data_fetch/stock_data_fetcher.py`
- 功能保持：日线数据/复权因子获取与落盘；若需要确保日历覆盖，可调用 `CalendarFetcher.ensure`。
- 说明：策略脚本不再调用该类的缺失检测；缺失评估由 Loader 提供。

## 脚本集成
- `scripts/run_strategies.py`：保持仅本地读取并跑策略；不触发网络或缺失检查。
- `scripts/run_fetcher.py`：
  - 使用 `StockDataLoader.check_missing_multi` 并发评估；若 `missing_count>0`，调用 `StockDailyKLineFetcher.get_daily_k_data` 补齐对应区间；
  - 在批量开始前，调用 `CalendarFetcher.ensure('SSE', today-365, today)` 进行一次预热，避免多次增量；
  - 参数 `--workers` 控制并发。

## 关键技术点（更新）
- Loader 不直接访问 Provider；当缓存日历缺失/不足时，调用 `CalendarFetcher.ensure` 获取并写入缓存，再继续本地评估。
- CalendarFetcher 仅处理交易日历，粒度支持年/月分块增量；写入采用原子替换；日志级别为 DEBUG（默认不刷屏）。
- 并发：Loader 的 `check_missing_multi` 与 run_fetcher 的并发补齐采用 `ThreadPoolExecutor`，在 I/O 密集场景提升吞吐。

## 验证
- 单测：
  - Loader 在三种场景（无本地/部分/完整）返回正确的 MissingInfo；
  - CalendarFetcher 的 ensure 在首次与增量调用下缓存文件正确更新；
- 集成：
  - 先清空部分代码的某段数据；运行 run_fetcher 并发补齐；再跑策略，确认不触发网络且数据完整。

## 变更列表
- 新增：`src/data_fetch/calendar_cache.py`
- 新增：`src/data_fetch/calendar_fetcher.py`
- 新增：`src/data_fetch/stock_data_loader.py`
- 更新：`scripts/run_fetcher.py` 采用 Loader + CalendarFetcher 协作；`README`/脚本说明补充职责划分与使用示例。