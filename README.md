# 股票分析系统

## 项目概述

基于Tushare接口的股票分析系统，实现股票数据获取、技术指标计算、策略筛选和数据可视化功能。

## 功能特点

1. **股票数据获取**
   - 爬取A股日线K线图
   - 支持爬取其他市场股票数据
   - 保存原始股价信息和复权因子

2. **股票数据分析**
   - 计算技术指标（BBI，MACD，RSI，KDJ等）
   - 提供自定义策略模板，用户可根据模板自定义策略筛选股票

3. **股票数据可视化**

4. **MCP Server**
   - 提供技术指标的MCP Server，支持通过聊天指定策略，执行策略筛选股票

## 项目结构

```
.
├── .env                 # 环境变量文件，存储API密钥等敏感信息
├── .trae/               # 项目配置文件目录
│   └── rules/           # 项目规则文件目录
│       └── project_rules.md  # 项目规则文件
├── data/                # 数据存储目录
│   ├── adj_factor/      # 存储股票复权因子信息
│   └── stock_data/      # 存储股票原始股价信息
├── src/                 # 项目源代码目录
│   ├── data_fetch/      # 数据获取模块
│   ├── indicators/      # 技术指标计算模块
│   ├── server/          # MCP Server模块
│   ├── strategies/      # 策略模块
│   ├── visualization/   # 可视化模块
│   └── config.py        # 配置文件
├── tests/               # 测试目录
└── README.md            # 项目说明文档
```

## 环境准备

### 1. 安装依赖

本项目使用uv管理环境，请确保已安装uv。

安装项目依赖：

```bash
uv install
```

### 2. 配置环境变量

复制`.env.example`文件为`.env`，并填写Tushare API密钥：

```bash
cp .env.example .env
```

编辑`.env`文件：

```
# Tushare API密钥
TUSHARE_TOKEN=your_tushare_token_here

# 数据存储路径
DATA_PATH=data/
```

## 数据下载

### 1. 运行数据下载脚本

#### 方法一：直接运行数据获取器模块

```bash
# 获取单只股票数据（例如：000001.SZ）
uv run python -c "from src.data_fetch.stock_data_fetcher import StockDataFetcher; fetcher = StockDataFetcher(); fetcher.get_daily_k_data('000001.SZ', start_date='20250101', end_date='20251231')"

# 批量获取多只股票数据
uv run python -c "from src.data_fetch.stock_data_fetcher import StockDataFetcher; fetcher = StockDataFetcher(); fetcher.get_multi_stocks_daily_k(['000001.SZ', '600000.SH'], start_date='20250101', end_date='20251231')"

# 爬取最近一年所有股票数据
uv run python -c "from src.data_fetch.stock_data_fetcher import StockDataFetcher; fetcher = StockDataFetcher(); fetcher.fetch_all_stocks_last_year()"

# 更新单只股票数据
uv run python -c "from src.data_fetch.stock_data_fetcher import StockDataFetcher; fetcher = StockDataFetcher(); fetcher.update_stock_data('000001.SZ')"

# 更新所有股票数据
uv run python -c "from src.data_fetch.stock_data_fetcher import StockDataFetcher; fetcher = StockDataFetcher(); fetcher.update_all_stocks_data()"
```

#### 方法二：使用测试脚本

创建测试脚本`test_fetch.py`：

```python
from src.data_fetch.stock_data_fetcher import StockDataFetcher

# 测试fetch功能
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 获取单只股票数据，测试是否能正确保存原始股价和复权因子
    df = fetcher.get_daily_k_data('000001.SZ', start_date='20251201', end_date='20251215', save_local=True)
    print("测试完成，查看data目录下是否生成了对应的文件")
```

运行测试脚本：

```bash
uv run python test_fetch.py
```

### 2. 数据存储结构

- **原始股价信息**：`data/stock_data/{股票代码}.csv`
  ```csv
  日期,开盘价,收盘价,最高价,最低价,成交量,成交额
  2025-12-01,11.6,11.69,11.7,11.53,1037322.71,1205480.323
  ```

- **复权因子信息**：`data/adj_factor/{股票代码}.csv`
  ```csv
  日期,复权因子
  2025-12-01,134.5794
  ```

## 技术指标计算

技术指标计算模块位于`src/indicators/technical_indicators.py`，支持计算BBI、MACD、RSI、KDJ等指标。

## 策略筛选

策略模块位于`src/strategies/base_strategy.py`，提供自定义策略模板，用户可根据模板自定义策略筛选股票。

## MCP Server

MCP Server模块位于`src/server/mcp_server.py`，提供技术指标的MCP Server，支持通过聊天指定策略，执行策略筛选股票。

## 可视化

可视化模块位于`src/visualization/stock_visualizer.py`，支持股票数据可视化。

## 开发说明

### 代码风格

- 使用Python 3.8+
- 遵循PEP 8代码风格
- 使用类型注解

### 测试

测试代码位于`tests/`目录，使用pytest进行测试：

```bash
uv run pytest
```

### 日志

项目使用Python内置logging模块，日志级别可在`src/config.py`中配置。

## 许可证

MIT License

## 免责声明

本项目仅用于学习和研究，不构成任何投资建议。股票投资有风险，入市需谨慎。