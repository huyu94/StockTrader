# Docker Compose 使用指南

## 什么是 Docker Compose？

**Docker Compose** 是一个用于定义和运行多容器 Docker 应用程序的工具。它使用 YAML 文件来配置应用程序的服务，然后通过一个简单的命令就可以创建并启动所有服务。

### 简单理解

- **Docker** = 运行单个容器
- **Docker Compose** = 管理多个容器，简化复杂的 docker run 命令

## 为什么使用 Docker Compose？

### 1. 简化命令

**不使用 Compose（复杂）**：
```bash
docker run --rm -it \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest
```

**使用 Compose（简单）**：
```bash
docker-compose up
```

### 2. 配置即代码

所有配置都写在 `docker-compose.yml` 文件中，可以版本控制，团队共享。

### 3. 管理多个服务

如果你的应用需要多个容器（比如：应用 + 数据库 + Redis），Compose 可以统一管理。

## 项目中的 docker-compose.yml 解析

让我们看看你项目中的配置：

```yaml
version: '3.8'  # Compose 文件格式版本

services:  # 定义服务（容器）
  stocktrader:  # 服务名称
    build: .  # 从当前目录的 Dockerfile 构建镜像
    container_name: stocktrader  # 容器名称
    
    volumes:  # 挂载目录（相当于 -v 参数）
      - .:/app  # 挂载当前目录到容器的 /app
      - ./data:/app/data  # 挂载数据目录
      - ./logs:/app/logs  # 挂载日志目录
      - ./output:/app/output  # 挂载输出目录
    
    environment:  # 环境变量（相当于 -e 参数）
      - PYTHONUNBUFFERED=1
      - PYTHONPATH=/app
      # - TUSHARE_TOKEN=${TUSHARE_TOKEN}  # 可以从 .env 文件读取
```

## 常用命令

### 基本命令

```bash
# 启动服务（前台运行，可以看到日志）
docker-compose up

# 启动服务（后台运行）
docker-compose up -d

# 停止服务
docker-compose down

# 停止并删除卷（数据也会被删除）
docker-compose down -v

# 查看运行状态
docker-compose ps

# 查看日志
docker-compose logs

# 查看实时日志
docker-compose logs -f

# 重新构建镜像并启动
docker-compose up --build

# 进入容器
docker-compose exec stocktrader /bin/bash
```

### 执行命令

```bash
# 在容器中执行命令
docker-compose exec stocktrader uv run python main.py

# 运行一次性命令（会创建新容器）
docker-compose run stocktrader uv run python scripts/fetch_kline_data.py
```

## 对比：docker run vs docker-compose

### 场景 1：启动容器

**docker run**：
```bash
docker run --rm -it \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest
```

**docker-compose**：
```bash
docker-compose up
```

### 场景 2：运行脚本

**docker run**：
```bash
docker run --rm \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -e PYTHONUNBUFFERED=1 \
  stocktrader:latest \
  uv run python scripts/fetch_kline_data.py
```

**docker-compose**：
```bash
docker-compose run stocktrader uv run python scripts/fetch_kline_data.py
```

## 实际使用示例

### 1. 开发模式

```bash
# 启动服务
docker-compose up

# 修改代码后，重启服务
docker-compose restart

# 或者重新构建并启动
docker-compose up --build
```

### 2. 运行脚本

```bash
# 运行数据获取脚本
docker-compose run stocktrader uv run python scripts/fetch_kline_data.py

# 运行策略脚本
docker-compose run stocktrader uv run python scripts/run_strategies.py
```

### 3. 进入容器调试

```bash
# 进入容器
docker-compose exec stocktrader /bin/bash

# 在容器内执行命令
docker-compose exec stocktrader uv run python -c "import pandas; print(pandas.__version__)"
```

## 多服务示例（扩展）

如果你的项目将来需要数据库，可以这样配置：

```yaml
version: '3.8'

services:
  stocktrader:
    build: .
    volumes:
      - .:/app
    environment:
      - DATABASE_URL=mysql://user:pass@db:3306/stocktrader
    depends_on:
      - db  # 依赖 db 服务
  
  db:  # MySQL 数据库服务
    image: mysql:8.0
    environment:
      - MYSQL_ROOT_PASSWORD=rootpassword
      - MYSQL_DATABASE=stocktrader
    volumes:
      - mysql_data:/var/lib/mysql

volumes:
  mysql_data:  # 持久化数据库数据
```

## 环境变量管理

### 使用 .env 文件

创建 `.env` 文件：
```env
TUSHARE_TOKEN=your_token_here
PYTHONUNBUFFERED=1
```

在 `docker-compose.yml` 中使用：
```yaml
environment:
  - TUSHARE_TOKEN=${TUSHARE_TOKEN}
```

## 总结

### Docker Compose 的优势

1. ✅ **简化命令**：一个命令启动所有服务
2. ✅ **配置管理**：所有配置在一个文件中
3. ✅ **版本控制**：配置可以提交到 Git
4. ✅ **团队协作**：团队成员使用相同的配置
5. ✅ **多服务管理**：轻松管理多个容器

### 什么时候使用？

- ✅ **开发环境**：简化启动流程
- ✅ **多服务应用**：需要多个容器协同工作
- ✅ **团队协作**：统一开发环境配置
- ✅ **CI/CD**：自动化部署流程

### 什么时候不需要？

- ❌ **单个简单容器**：直接用 `docker run` 就够了
- ❌ **生产环境**：可能需要 Kubernetes 等更强大的工具

## 快速参考

```bash
# 最常用的命令
docker-compose up          # 启动
docker-compose down        # 停止
docker-compose logs -f     # 查看日志
docker-compose exec stocktrader /bin/bash  # 进入容器
```

