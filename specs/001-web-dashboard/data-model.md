# Data Model: Web Dashboard

## Stock
**Represents**: 股票基础信息  
**Key fields**:
- ts_code: 股票代码
- name: 股票名称
- market: 市场/交易所
- status: 上市状态

## DailyKline
**Represents**: 日线行情记录  
**Key fields**:
- ts_code: 股票代码
- trade_date: 交易日期
- open: 开盘价
- high: 最高价
- low: 最低价
- close: 收盘价
- volume: 成交量
- amount: 成交额
- adj_factor: 复权因子（若可用）

## DataUpdateStatus
**Represents**: 数据更新状态  
**Key fields**:
- dataset: 数据集名称（如 daily_kline）
- last_updated_at: 最近更新时间
- coverage_start: 覆盖开始日期
- coverage_end: 覆盖结束日期
- status: 运行状态（ok / warning / error）
- message: 状态说明

## UiActionSlot
**Represents**: 预留采集按钮入口  
**Key fields**:
- id: 入口标识
- label: 按钮名称
- state: disabled / enabled / coming_soon
- tooltip: 未启用提示文案
