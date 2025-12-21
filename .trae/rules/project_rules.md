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

1. providers文件夹，提供数据获取的接口，目前只有tushare_api：
    1. base_provider：provider基类
    2. tushare_provider：tushare api provider

2. fetchers文件夹
    1. daily_kline_fetcher：获取股票的日K线行情数据, 使用pro.daily数据接口
    2. adj_factor_fetcher：获取股票的复权因子数据

3. loaders文件夹
    1. 加载本地数据
    2. 计算缺失矩阵，行=交易日，列=股票代码；值=是否本地缺失，返回缺失矩阵

4. 关于缺失矩阵:
    1. 缺失矩阵是一个二值矩阵，行=交易日，列=股票代码；值=是否本地缺失
    2. 加载本地股票数据，填充缺失矩阵，缺失矩阵的每个元素表示对应交易日是否有该股票的本地数据。
    3. 对于一个交易日，如果缺失股票数量达到阈值1000，就用pro.daily接口获取该交易日的所有股票数据。
    4. 如果没有达到阈值，就暂时跳过。
    5. 遍历完所有交易日后，根据列来遍历，爬取最小缺失交易日、最大缺失交易日的股票数据。

# 代码原则
