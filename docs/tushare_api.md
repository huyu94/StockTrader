# 文档使用说明
- 这个文档介绍了tushare的一些api使用说明



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


# 通用行情接口
接口名称：pro_bar，本接口是集成开发接口，部分指标是现用现算
更新时间：股票和指数通常在15点～17点之间，数字货币实时更新，具体请参考各接口文档明细。
描述：目前整合了股票（未复权、前复权、后复权）、指数、数字货币、ETF基金、期货、期权的行情数据，未来还将整合包括外汇在内的所有交易行情数据，同时提供分钟数据。不同数据对应不同的积分要求，具体请参阅每类数据的文档说明。
其它：由于本接口是集成接口，在SDK层做了一些逻辑处理，目前暂时没法用http的方式调取通用行情接口。用户可以访问Tushare的Github，查看源代码完成类似功能。

输入参数

名称	类型	必选	描述
ts_code	str	Y	证券代码，不支持多值输入，多值输入获取结果会有重复记录
start_date	str	N	开始日期 (日线格式：YYYYMMDD，提取分钟数据请用2019-09-01 09:00:00这种格式)
end_date	str	N	结束日期 (日线格式：YYYYMMDD)
asset	str	Y	资产类别：E股票 I沪深指数 C数字货币 FT期货 FD基金 O期权 CB可转债（v1.2.39），默认E
adj	str	N	复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None，目前只支持日线复权，同时复权机制是根据设定的end_date参数动态复权，采用分红再投模式，具体请参考常见问题列表里的说明。
freq	str	Y	数据频度 ：支持分钟(min)/日(D)/周(W)/月(M)K线，其中1min表示1分钟（类推1/5/15/30/60分钟） ，默认D。对于分钟数据有600积分用户可以试用（请求2次），正式权限可以参考权限列表说明 ，使用方法请参考股票分钟使用方法。
ma	list	N	均线，支持任意合理int数值。注：均线是动态计算，要设置一定时间范围才能获得相应的均线，比如5日均线，开始和结束日期参数跨度必须要超过5日。目前只支持单一个股票提取均线，即需要输入ts_code参数。e.g: ma_5表示5日均价，ma_v_5表示5日均量
factors	list	N	股票因子（asset='E'有效）支持 tor换手率 vr量比
adjfactor	str	N	复权因子，在复权数据时，如果此参数为True，返回的数据中则带复权因子，默认为False。 该功能从1.2.33版本开始生效



# 分红送股
接口：dividend
描述：分红送股数据
权限：用户需要至少2000积分才可以调取，具体请参阅积分获取办法

## 输入参数
名称	类型	必选	描述
ts_code	str	N	TS代码
ann_date	str	N	公告日
record_date	str	N	股权登记日期
ex_date	str	N	除权除息日
imp_ann_date	str	N	实施公告日
以上参数至少有一个不能为空

## 输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
end_date	str	Y	分红年度
ann_date	str	Y	预案公告日
div_proc	str	Y	实施进度
stk_div	float	Y	每股送转
stk_bo_rate	float	Y	每股送股比例
stk_co_rate	float	Y	每股转增比例
cash_div	float	Y	每股分红（税后）
cash_div_tax	float	Y	每股分红（税前）
record_date	str	Y	股权登记日
ex_date	str	Y	除权除息日
pay_date	str	Y	派息日
div_listdate	str	Y	红股上市日
imp_ann_date	str	Y	实施公告日
base_date	str	N	基准日
base_share	float	N	基准股本（万）

## 接口示例
```
pro = ts.pro_api()

df = pro.dividend(ts_code='600848.SH', fields='ts_code,div_proc,stk_div,record_date,ex_date')
```
## 数据样例
```
ts_code div_proc  stk_div record_date   ex_date
0  600848.SH       实施     0.10    19950606  19950607
1  600848.SH       实施     0.10    19970707  19970708
2  600848.SH       实施     0.15    19960701  19960702
3  600848.SH       实施     0.10    19980706  19980707
4  600848.SH       预案     0.00        None      None
5  600848.SH       实施     0.00    20180522  20180523
```