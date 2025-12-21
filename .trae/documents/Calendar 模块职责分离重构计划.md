# 实现 Calendar 模块的职责分离

根据您的要求，我们将引入 `CalendarManager` 来协调 `Loader` 和 `Fetcher`，实现数据的按需加载与自动更新。

## 1. 创建 `src/loaders/calendar_loader.py`
负责本地数据的读取与状态检查。
- **功能**：
    - `load(exchange)`: 读取 `data/{exchange}_trade_calendar.csv`，返回 DataFrame。
    - `need_update(exchange)`: 检查 `cache/calendar_{exchange}_update.json`。
        - 如果缓存文件或数据文件不存在，返回 `True`。
        - 如果 `last_updated_at` 非今日，返回 `True`。

## 2. 实现 `src/fetchers/calendar_manager.py`
作为统一入口，管理日历数据的生命周期。
- **功能**：
    - 组合 `CalendarLoader` 和 `CalendarFetcher`。
    - 使用 `@cached_property` 暴露接口：
        - `sse_calendar`: 检查 loader 状态 -> 按需调用 fetcher -> 返回 loader 数据。
        - `szse_calendar`: 同上。
    - 确保对外提供的是最新的、已缓存的 DataFrame。

## 3. 验证与测试
- 创建 `tests/test_calendar_manager.py`。
- 模拟场景：
    - 本地无数据 -> Manager 自动触发 Fetcher 拉取 -> 返回数据。
    - 本地有数据但过期 -> Manager 自动触发 Fetcher 更新 -> 返回新数据。
    - 本地数据最新 -> Manager 直接从 Loader 读取，不触发网络请求。
