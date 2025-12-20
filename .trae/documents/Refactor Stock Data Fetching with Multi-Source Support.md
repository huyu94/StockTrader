基于对现有项目的阅读以及对开源量化工具（如 OpenBB, AkShare）的调研，我制定了以下重构与改进计划。

### 1. 调研与改进思路
- **调研发现**：优秀的开源量化工具通常采用“多数据源适配器模式”，将数据获取与业务逻辑分离。**AkShare** 是目前 Python 社区非常活跃的开源财经数据接口库，免费且覆盖面广，非常适合作为本项目的补充数据源。
- **改进方案**：在 `src/data_fetch` 文件夹下新增对 AkShare 的支持，并引入 `tenacity` 库来处理网络请求重试，提升稳定性。

### 2. 架构重构：严格遵循“单只 -> 批量”模式
根据您的要求，我将把数据获取模块重构为三层结构：

- **第一层：基础原子能力 (Providers)**
  - 创建 `src/data_fetch/providers/` 目录。
  - 定义标准接口：所有 Provider 必须实现“获取单只股票数据”的方法。
  - **TushareProvider**：迁移现有的 Tushare 逻辑。
  - **AkShareProvider**（新增）：实现 AkShare 的单只股票获取逻辑，并将字段映射为项目统一格式。

- **第二层：单只股票管理 (Single Stock Manager)**
  - 在 `StockDailyKLineFetcher` 中完善 `fetch_single_stock(ts_code)` 方法。
  - 该方法只做一件事：**处理单只股票的生命周期**（检查本地缓存 -> 本地缺失则调用 Provider 获取 -> 保存到本地）。

- **第三层：批量获取扩展 (Batch Extension)**
  - 在 `StockDailyKLineFetcher` 中实现 `fetch_all_stocks()`。
  - 该方法**不包含任何数据获取逻辑**，仅仅是通过循环或线程池（ThreadPoolExecutor）去并发调用第二层的 `fetch_single_stock` 方法，从而实现从单只到批量的扩展。

### 3. 实施步骤
1.  **依赖管理**：使用 `uv` 添加 `akshare` 和 `tenacity`。
2.  **代码实现**：
    - 创建 `providers` 模块及基类。
    - 实现 `akshare_provider.py` 和 `tushare_provider.py`。
    - 重构 `stock_data_fetcher.py` 以使用新的 Provider 模式。
3.  **验证**：
    - 编写测试脚本验证 AkShare 能否正确获取并格式化数据。
    - 运行现有测试确保重构未破坏原有功能。
