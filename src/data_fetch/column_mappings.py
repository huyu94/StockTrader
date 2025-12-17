# 列名映射字典，统一管理Tushare API字段到中文列名的映射

# pro.daily接口输出字段映射
DAILY_COLUMN_MAPPINGS = {
    'trade_date': '日期',
    'open': '开盘价',
    'close': '收盘价',
    'high': '最高价',
    'low': '最低价',
    'pre_close': '昨收价',
    'change': '涨跌额',
    'pct_chg': '涨跌幅',
    'vol': '成交量',
    'amount': '成交额'
}

# pro.adj_factor接口输出字段映射
ADJ_FACTOR_COLUMN_MAPPINGS = {
    'trade_date': '日期',
    'adj_factor': '复权因子'
}