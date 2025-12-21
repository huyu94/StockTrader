# 股票分析系统

## 项目概述

基于Tushare接口的股票分析系统，实现股票数据获取、技术指标计算、策略筛选和数据可视化功能。

## 数据获取架构

### 架构概览

系统采用分层架构设计，从底层到顶层分为：**Provider层** → **Fetcher层** → **Storage层** → **Manager层**。

```
┌─────────────────────────────────────────────────────────────┐
│                      Manager (统一管理器)                     │
│  - 协调数据获取流程                                           │
│  - 管理线程池（IO线程池、任务线程池）                          │
│  - 智能更新策略（全量更新/增量更新）                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Fetcher (数据获取层)                       │
│  - DailyKlineFetcher: 日线行情获取                           │
│  - AdjFactorFetcher: 复权因子获取（已弃用，现包含在日线中）    │
│  - BasicInfoFetcher: 股票基本信息获取                         │
│  - CalendarFetcher: 交易日历获取                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Provider (API接口层)                       │
│  - TushareProvider: Tushare API封装                          │
│    • 单例模式，确保API调用串行                                │
│    • 使用锁机制避免IP超限                                     │
│    • 支持 pro_bar API（快速批量获取）                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Storage (数据存储层)                        │
│  - DailyKlineStorageSQLite: 日线数据SQLite存储                │
│  - AdjFactorStorageSQLite: 复权因子SQLite存储                 │
│  - BasicInfoStorageSQLite: 基本信息SQLite存储                 │
│  - CalendarStorageSQLite: 交易日历SQLite存储                  │
│    • 统一使用SQLite数据库，性能提升5-15倍                      │
│    • 支持批量写入，单次事务写入所有数据                        │
│    • 自动去重（INSERT OR REPLACE）                            │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件详解

#### 1. Provider层 (`src/providers/`)

**TushareProvider** - Tushare API封装类

- **单例模式**：确保全局只有一个API实例
- **串行调用**：使用 `threading.Lock` 确保所有API调用完全串行，避免IP超限
- **性能优化**：
  - 设置 `NO_PROXY` 环境变量，绕过系统代理提高速度
  - 使用 `pro_bar` API，一次调用获取单只股票全部历史数据
  - 支持 `factors` 参数，同时获取复权因子

**关键方法**：
- `pro_bar()`: 使用 pro_bar API 获取K线数据（推荐，更快）
- `query()`: 通用查询接口
- `daily()`: 使用 daily API 获取日线数据（用于按日期批量获取）

#### 2. Fetcher层 (`src/fetchers/`)

**DailyKlineFetcher** - 日线行情获取器

- `fetch_one()`: 获取单只股票的日线数据
  - 使用 `pro_bar` API，一次获取全部历史
  - 自动获取复权因子（`factors="tor"`）
  - 统一列名处理（`factor` → `adj_factor`）

- `fetch_daily_by_date()`: 按日期获取全市场数据
  - 用于增量更新场景
  - 当某个交易日缺失数据较多时使用
  - 使用 `daily` API（pro_bar不支持按日期获取全市场数据）

**其他Fetcher**：
- `BasicInfoFetcher`: 获取股票基本信息
- `CalendarFetcher`: 获取交易日历
- `AdjFactorFetcher`: 复权因子获取（已弃用，现包含在日线数据中）

#### 3. Storage层 (`src/storage/`)

**SQLite存储架构**

所有数据统一存储在 `data/stock_data.db` SQLite数据库中，包含以下表：

- `daily_kline`: 日线行情数据（包含复权因子）
- `adj_factor`: 复权因子数据（保留，但已包含在daily_kline中）
- `basic_info`: 股票基本信息
- `trade_calendar`: 交易日历

**性能优势**：
- **批量写入**：单次事务写入所有数据，性能提升5-15倍
- **自动去重**：使用 `INSERT OR REPLACE` 自动处理重复数据
- **索引优化**：为常用查询字段创建索引
- **WAL模式**：启用Write-Ahead Logging，提升并发性能

**SQLiteBaseStorage** - 存储基类
- 提供统一的数据库连接管理
- 实现 `_upsert_method`，支持批量UPSERT操作
- 自动处理NaN值转换

#### 4. Manager层 (`src/managers/`)

**Manager** - 统一数据管理器

**核心功能**：
- 统一管理所有数据类型（日线、复权因子、基础信息、日历）
- 维护线程池：
  - `io_executor`: 20个工作线程，用于密集文件IO操作
  - `task_executor`: 1个工作线程，用于后台任务调度

**智能更新策略**：

1. **全量更新（按股票代码）** - `_update_all_stocks_full()`
   - 适用场景：首次爬取数据
   - 流程：遍历所有股票 → 获取最近一年数据 → 覆盖写入
   - 优势：数据完整，适合初始化

2. **增量更新（按交易日）** - `_update_missing_data_incremental()`
   - 适用场景：定期更新数据
   - 流程：
     - 生成数据存在性矩阵（交易日 × 股票代码）
     - 识别缺失数据较多的交易日（阈值：1000只股票）
     - 批量获取该日所有股票数据
     - 批量写入数据库
   - 优势：只更新缺失数据，效率高

**Matrix Manager** (`src/common/`)
- `DataMatrixManager`: 管理日线数据存在性矩阵
- `AdjFactorMatrixManager`: 管理复权因子数据存在性矩阵

### 数据流程

#### 全量更新流程（首次爬取）

```
1. Manager.update_daily_kline()
   ↓
2. 检查是否有历史数据
   ↓ (无历史数据)
3. _update_all_stocks_full()
   ↓
4. 获取所有股票代码列表
   ↓
5. 遍历每只股票（并发）
   ├─→ DailyKlineFetcher.fetch_one()
   │   └─→ TushareProvider.pro_bar()
   │       └─→ 调用Tushare API
   └─→ DailyKlineStorageSQLite.write_one()
       └─→ SQLite批量写入
```

#### 增量更新流程（定期更新）

```
1. Manager.update_daily_kline()
   ↓
2. 检查是否有历史数据
   ↓ (有历史数据)
3. _update_missing_data_incremental()
   ↓
4. DataMatrixManager.generate_matrix()
   ↓ (生成缺失矩阵)
5. 遍历交易日，识别缺失数据较多的日期
   ↓
6. 对于缺失数 > 1000 的交易日：
   ├─→ DailyKlineFetcher.fetch_daily_by_date()
   │   └─→ TushareProvider.query("daily")
   └─→ DailyKlineStorageSQLite.write_batch()
       └─→ SQLite批量写入（单次事务）
```

### 性能优化

1. **API调用优化**
   - 使用 `pro_bar` API，一次获取单只股票全部历史（比多次调用 `daily` 更快）
   - 串行调用避免IP超限
   - 绕过代理提高速度

2. **存储优化**
   - SQLite批量写入，单次事务写入所有数据
   - 减少文件打开/关闭开销（从6000次减少到1次）
   - 性能提升：5-15倍

3. **并发优化**
   - IO线程池：20个工作线程处理文件写入
   - 任务线程池：1个工作线程调度后台任务
   - 流水线处理：获取和写入并行进行

### 使用方式

#### 初始化数据库

```bash
# 创建所有数据库表
uv run scripts/init_sqlite_db.py
```

#### 数据爬取

```bash
# 全量更新（按股票代码，首次爬取）
uv run scripts/run_fetch.py --mode code --lookback-days 365

# 增量更新（按交易日，定期更新，默认）
uv run scripts/run_fetch.py --mode date --lookback-days 365
```

#### 查看数据

```bash
# 查看指定股票数据
uv run scripts/view_sqlite_data.py --ts-code 000001.SZ

# 查看数据库统计信息
uv run scripts/view_sqlite_data.py --stats
```

### 项目结构

```
.
├── src/
│   ├── providers/          # API接口层
│   │   ├── base_provider.py
│   │   └── tushare_provider.py
│   ├── fetchers/           # 数据获取层
│   │   ├── daily_kline_fetcher.py
│   │   ├── adj_factor_fetcher.py
│   │   ├── basic_info_fetcher.py
│   │   └── calendar_fetcher.py
│   ├── storage/            # 数据存储层
│   │   ├── sqlite_base.py
│   │   ├── daily_kline_storage_sqlite.py
│   │   ├── adj_factor_storage_sqlite.py
│   │   ├── basic_info_storage_sqlite.py
│   │   └── calendar_storage_sqlite.py
│   ├── managers/           # 统一管理层
│   │   └── manager.py
│   └── common/             # 公共模块
│       ├── data_matrix_manager.py
│       └── adj_factor_matrix_manager.py
├── scripts/
│   ├── run_fetch.py        # 数据爬取脚本
│   ├── init_sqlite_db.py   # 数据库初始化脚本
│   └── view_sqlite_data.py # 数据查看脚本
└── data/
    └── stock_data.db       # SQLite数据库文件
```

### 环境准备

#### 1. 安装依赖

```bash
uv install
```

#### 2. 配置环境变量

创建 `.env` 文件：

```env
# Tushare API密钥
TUSHARE_TOKEN=your_tushare_token_here

# 数据存储路径（可选）
DATA_PATH=data/
```

### 技术栈

- **Python 3.8+**
- **Tushare Pro API**: 股票数据源
- **SQLite**: 数据存储（性能优化）
- **Pandas**: 数据处理
- **ThreadPoolExecutor**: 并发处理
- **Loguru**: 日志管理

### 关键特性

1. **智能更新策略**：自动选择全量更新或增量更新
2. **高性能存储**：SQLite批量写入，性能提升5-15倍
3. **API优化**：使用 pro_bar API，一次获取全部历史数据
4. **复权因子集成**：复权因子已包含在日线数据中，无需单独爬取
5. **线程安全**：使用锁机制确保API调用串行，避免IP超限

### 注意事项

1. **API限制**：Tushare API有调用频率限制，系统已通过串行调用避免超限
2. **数据完整性**：首次爬取建议使用 `--mode code` 全量更新
3. **定期更新**：建议每日使用 `--mode date` 增量更新
4. **数据库备份**：SQLite数据库文件位于 `data/stock_data.db`，建议定期备份

### 许可证

MIT License

### 免责声明

本项目仅用于学习和研究，不构成任何投资建议。股票投资有风险，入市需谨慎。
