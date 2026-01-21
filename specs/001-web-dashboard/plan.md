# Implementation Plan: Web Dashboard

**Branch**: `001-web-dashboard` | **Date**: 2026-01-21 | **Spec**: `specs/001-web-dashboard/spec.md`
**Input**: Feature specification from `specs/001-web-dashboard/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

交付一个面向日线行情的前后端可视化界面，包含股票搜索、日线图展示、数据更新状态与
预留采集按钮区。技术实现采用 React + Ant Design + ECharts 作为前端，FastAPI 作为
后端，提供只读 REST API 与前端对接。

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**Language/Version**: Python 3.12（后端），Node.js 20+ 与 TypeScript 5.x（前端）  
**Primary Dependencies**: FastAPI, Uvicorn, SQLAlchemy, Pydantic；React, Ant Design, ECharts  
**Storage**: MySQL 为主（沿用 ETL 数据库），本地开发可选 SQLite  
**Testing**: pytest（后端），前端测试暂不做强制要求  
**Target Platform**: Web 浏览器 + Linux 服务器  
**Project Type**: Web application  
**Performance Goals**: 95% 日线图请求在 3 秒内完成展示  
**Constraints**: 使用 uv 管理 Python 环境；日志使用 loguru；数据处理以 pandas 为主  
**Scale/Scope**: 单业务域仪表盘，预计 1-3 个页面起步

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

当前 Constitution 仍为模板占位，暂无可执行的硬性 Gate。默认通过，后续如补充规范需回查。

## Project Structure

### Documentation (this feature)

```text
specs/001-web-dashboard/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
backend/
├── src/
│   ├── api/
│   ├── models/
│   └── services/
└── tests/

frontend/
├── src/
│   ├── components/
│   ├── pages/
│   └── services/
└── tests/
```

**Structure Decision**: Web application，分离 `backend/` 与 `frontend/` 便于独立构建与部署。

## Complexity Tracking

无额外复杂度或违反项需要说明。
