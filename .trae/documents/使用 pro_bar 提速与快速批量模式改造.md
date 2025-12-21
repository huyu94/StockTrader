## 目标
- 用 `ts.pro_bar` 抓取前复权日线，CSV 保留原始 API 列名（不映射中文）。
- 在 `column_mappings` 中新增“API→中文”映射；Strategies 在输出阶段再做映射。
- Fetcher 统一默认抓取近一年，可传入 `start_date/end_date` 覆盖。

## 实施内容
1. Provider 改造
- `TushareProvider.get_daily_k_data(ts_code, start_date, end_date)`：
  - 调用：`ts.pro_bar(ts_code=..., start_date=..., end_date=..., adj="qfq", freq="D", api=self.pro)`
  - 返回不改列名（保持 API 原始列，如：`trade_date, open, close, high, low, vol`）
  - 排序与类型转换（`trade_date` 转为字符串或 pandas datetime，由 Fetcher 落盘时统一）

2. Fetcher 统一行为
- `fetch_one(ts_code, start_date=None, end_date=None, save_local=True)` 与 `fetch_batch(ts_codes, start_date=None, end_date=None, workers=6, save_local=True)`：
  - 未指定时间则自动近一年；并发默认 6
  - 覆盖保存 CSV，保留原始列名；最简处理（不做合并去重/列名映射）
  - 输出每只的耗时与总耗时

3. Column Mappings
- 在 `src/data/common/column_mappings.py` 增加映射：
  - `API_DAILY_MAPPINGS = {"trade_date": "交易日期", "open": "开盘价", "close": "收盘价", "high": "最高价", "low": "最低价", "vol": "成交量"}`
- Strategies 在预处理/输出时使用 `API_DAILY_MAPPINGS` 将英文列映射为中文（保持当前策略结构，只是映射源从 API 英文列）

4. 脚本简化
- `scripts/run_fetcher.py`：移除交易日历与缺失检查，直接 `fetch_batch(codes[:limit], workers=args.workers)`
- 继续共享 Provider 单例注入；日志按 INFO/DEBUG 区分（config.py）

5. 文档
- README：记录“CSV 保持英文列，策略输出映射到中文”与使用示例
- `.trae/rules/project_rules.md`：同步 uv、loguru、pandas、pytests、tests 目录、需求入文档的约定

## 验证
- `uv run python scripts/run_fetcher.py --provider tushare --workers 6 --limit 200`
- 检查生成 CSV 列是否为英文；运行策略，确认输出为中文列名。
