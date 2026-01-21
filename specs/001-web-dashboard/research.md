# Research: Web Dashboard

## Decision 1: 前端技术栈
**Decision**: React + Ant Design + ECharts + TypeScript  
**Rationale**: Ant Design 适合数据密集型后台；ECharts 对 K 线与成交量图成熟；React
生态广、复用组件多。  
**Alternatives considered**: Vue3 + Element Plus + ECharts（更传统后台风格，但组件生态略弱）；
React + Plotly（交互强但定制成本与性能压力更高）。

## Decision 2: 后端技术栈
**Decision**: FastAPI + SQLAlchemy + Pydantic + Uvicorn  
**Rationale**: 与现有 Python ETL 代码一致，易复用数据模型与业务逻辑，API 文档自动生成。  
**Alternatives considered**: Django（功能完整但较重），Flask（生态轻但结构化与类型约束不足）。

## Decision 3: 接口形态
**Decision**: REST API（只读为主）  
**Rationale**: 前端与数据展示的匹配度高，易于缓存与分页。  
**Alternatives considered**: GraphQL（灵活但实现复杂度更高）。

## Decision 4: 数据来源与读写策略
**Decision**: 直接读取现有 ETL 落库的日线数据；不在 UI 侧写入  
**Rationale**: MVP 阶段以展示为主，避免引入交易或数据更新写入风险。  
**Alternatives considered**: UI 侧触发写入（需要鉴权与审计，超出当前范围）。

## Decision 5: 预留采集按钮策略
**Decision**: 固定位置展示多个入口，默认为禁用或“未启用”提示  
**Rationale**: 保持布局稳定，降低未来集成改版成本。  
**Alternatives considered**: 动态渲染入口（初期实现成本更高，且不符合“先预留”的诉求）。
