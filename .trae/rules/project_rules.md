# 环境管理
1. 用uv来管理python, uv run xx.py来运行项目， uv add来添加依赖

# 包
1. 使用loguru来记录日志
2. 使用pandas来处理数据


# 测试
1. 使用pytests
2. 所有tests文件都写到tests目录下，每个test文件都以test_开头，例如test_fetch.py


# 关于文档
1. 在和你对话的过程中，需要你将我的需求整理更新到这个文档中。
2. 把整个项目的构建思路、功能描述、数据结构，都写到README.md文件中。

# 项目设计
1. 交易日日历:
    1. 1. calendar_fetcher: 从tushare api爬取交易日历数据
    1. 2. calendar_loader: 加载交易日历数据到本地数据库
    1. 3. calendar_manager: 管理交易日历数据，确保交易日期覆盖到查询日至一年前
2. 复权因子:
    2. 1. adj_factor_fetcher: 从tushare api爬取股票的复权因子数据
    2. 2. adj_factor_loader: 加载复权因子数据到本地数据库
    2. 3. adj_factor_manager: 管理复权因子数据，确保交易日期覆盖到日
3. 股票日k线数据：
    3. 1. daily_kline_fetcher: 从tushare api爬取股票的日K线行情数据
    3. 2. daily_kline_loader: 加载股票的日K线行情数据到本地数据库
    3. 3. daily_kline_manager: 管理股票的日K线行情数据，确保交易日期覆盖到查询日至一年前
1. 本地存储：
    1. 1. 某些交易所股票基本数据（目前SSE， SZSE）：tushare api爬取
    1. 2. 股票的复权因子数据：tushare api爬取
    1. 3. 股票的日K线行情数据：tushare api爬取
    1. 4. 交易日历数据：tushare api爬取
    1. 5. 交易数据透视图（本地生成，行为交易日期，列为股票代码， 表示该股票在该日期的数据是否存在本地）
    1. 6. 股票前复权数据：前面指标计算
2. 爬取-fetcher：
    2. 1. CalendarFetcher从tushare api爬取股票基本数据
    2. 2. AdjFactorFetcher从tushare api爬取股票的复权因子数据
    2. 3. DailyKlineFetcher从tushare api爬取股票的日K线行情数据
    2. 4. CalendarFetcher从tushare api爬取交易日历数据
3. provider：
    3. 1. Tushare provider

4. 限流机制：
    4. 1. 把现在限流机制放到Tushare provider中，网络请求限制在同ip最多两个连接，每分钟500次请求。

5. run_fetcher重构（近一年快速补齐）：
    5. 1. 计算“交易日缺失表格”（行=交易日，列=股票代码；值=是否本地缺失）
    5. 2. 优先按行处理：当某交易日缺失股票数>阈值（默认1000），调用`pro.daily(trade_date=...)`一次性抓该日所有股票，并按`ts_code`写入各自CSV
    5. 3. 对剩余小缺口，再按股票维度进行补齐（按缺失最早日期作为`start_date`）
    5. 4. Provider层统一限流与并发控制，Fetcher层负责缺口检测与落盘幂等
    5. 5. 复权因子同理：新增`run_adj_factor_fetcher.py`，在“大缺口日”用`pro.adj_factor(trade_date=...)`抓取当日所有股票的复权因子，再按股票维度补齐。



# 代码原则
