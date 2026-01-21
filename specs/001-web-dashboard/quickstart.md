# Quickstart: Web Dashboard

## Prerequisites
- Python 3.12 (use `uv` for environment management)
- Node.js 20+ (frontend build)
- 数据库已配置（通过环境变量 `DATABASE_URL` 或 MySQL 相关环境变量）

## Backend

### 安装依赖
```bash
uv sync
```

### 启动服务
```bash
uv run uvicorn backend.src.main:app --reload --host 0.0.0.0 --port 8000
```

服务将在 `http://localhost:8000` 启动，API 文档可在 `http://localhost:8000/docs` 查看。

## Frontend

### 安装依赖
```bash
cd frontend
npm install
```

### 启动开发服务器
```bash
npm run dev
```

前端将在 `http://localhost:5173` 启动，并自动代理 `/api` 请求到后端。

## 环境变量配置

后端需要配置数据库连接，可通过以下方式之一：

1. **直接指定数据库URL**：
   ```bash
   export DATABASE_URL="mysql+pymysql://user:password@host:port/database?charset=utf8mb4"
   ```

2. **使用MySQL环境变量**：
   ```bash
   export MYSQL_HOST=localhost
   export MYSQL_USER=root
   export MYSQL_PASSWORD=password
   export MYSQL_DATABASE=stock_data
   export MYSQL_PORT=3306
   ```

3. **使用SQLite（默认）**：
   ```bash
   export SQLITE_PATH="data/stock_data.db"
   ```

## API 端点

- `GET /api/health` - 健康检查
- `GET /api/stocks` - 搜索股票列表
- `GET /api/stocks/{ts_code}/daily` - 获取股票日线行情
- `GET /api/status/data-updates` - 获取数据更新状态
- `GET /api/ui/action-slots` - 获取预留采集按钮入口

## Notes
- 后端提供只读 API（股票列表、日线行情、数据更新状态、预留按钮入口）
- 前端通过 Vite 代理访问 `/api` 前缀的接口
- 所有 API 响应使用 JSON 格式
