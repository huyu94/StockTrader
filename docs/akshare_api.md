# akshare api 数据

实时行情数据
实时行情数据-东财
沪深京 A 股
接口: stock_zh_a_spot_em

目标地址: https://quote.eastmoney.com/center/gridlist.html#hs_a_board

描述: 东方财富网-沪深京 A 股-实时行情数据

限量: 单次返回所有沪深京 A 股上市公司的实时行情数据

输入参数

名称	类型	描述
-	-	-
输出参数
名称	类型	描述
序号	int64	-
代码	object	-
名称	object	-
最新价	float64	-
涨跌幅	float64	注意单位: %
涨跌额	float64	-
成交量	float64	注意单位: 手
成交额	float64	注意单位: 元
振幅	float64	注意单位: %
最高	float64	-
最低	float64	-
今开	float64	-
昨收	float64	-
量比	float64	-
换手率	float64	注意单位: %
市盈率-动态	float64	-
市净率	float64	-
总市值	float64	注意单位: 元
流通市值	float64	注意单位: 元
涨速	float64	-
5分钟涨跌	float64	注意单位: %
60日涨跌幅	float64	注意单位: %
年初至今涨跌幅	float64	注意单位: %
接口示例
```
import akshare as ak
stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
print(stock_zh_a_spot_em_df)
```