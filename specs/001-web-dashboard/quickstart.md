# Quickstart: Web Dashboard

## Prerequisites
- Python 3.12 (use `uv` for environment management)
- Node.js 20+ (frontend build)

## Backend (planned)
```bash
uv install
uv run uvicorn backend.src.main:app --reload
```

## Frontend (planned)
```bash
cd frontend
npm install
npm run dev
```

## Notes
- 后端提供只读 API（股票列表、日线行情、数据更新状态、预留按钮入口）
- 前端访问 `/api` 前缀的接口
