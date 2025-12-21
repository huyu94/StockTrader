
# pro.daily
接口：daily，可以通过数据工具调试和查看数据
数据说明：交易日每天15点～16点之间入库。本接口是未复权行情，停牌期间不提供数据
调取说明：基础积分每分钟内可调取500次，每次6000条数据，一次请求相当于提取一个股票23年历史
描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据
输入参数
名称	类型	必选	描述
ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
trade_date	str	N	交易日期（YYYYMMDD）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数
名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
open	float	开盘价
high	float	最高价
low	float	最低价
close	float	收盘价
pre_close	float	昨收价【除权价，前复权】
change	float	涨跌额
pct_chg	float	涨跌幅 【基于除权后的昨收计算的涨跌幅：（今收-除权昨收）/除权昨收 】
vol	float	成交量 （手）
amount	float	成交额 （千元）


# 复权因子
接口：adj_factor，可以通过数据工具调试和查看数据。
更新时间：盘前9点15~20分完成当日复权因子入库
描述：本接口由Tushare自行生产，获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
积分要求：2000积分起，5000以上可高频调取

输入参数
名称	类型	必选	描述
ts_code	str	N	股票代码
trade_date	str	N	交易日期(YYYYMMDD，下同)
start_date	str	N	开始日期
end_date	str	N	结束日期
注：日期都填YYYYMMDD格式，比如20181010

输出参数
名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
adj_factor	float	复权因子