# Docker 使用指南

## Docker 核心流程

Docker 的基本使用流程非常简单：

```
docker build → [选择路径]
                ├─→ docker run      (本地运行容器)
                └─→ docker save     (导出镜像，用于传输/备份)
```

## 第一步：构建镜像 (docker build)

### 命令说明

```bash
docker build -t stocktrader:latest .
```

**参数解释**：
- `docker build`：构建镜像的命令
- `-t stocktrader:latest`：为镜像打标签（命名）
  - `stocktrader`：镜像名称
  - `:latest`：标签（版本标识，latest 是默认标签）
- `.`：构建上下文路径（当前目录）

### 构建过程

1. Docker 读取当前目录的 `Dockerfile`
2. 按照 Dockerfile 的指令逐步构建镜像
3. 镜像存储在 Docker 的本地存储中（不是文件系统中的文件）

### 查看构建的镜像

```bash
# 查看所有镜像
docker images

# 查看特定镜像
docker images stocktrader

# 输出示例：
# REPOSITORY    TAG       IMAGE ID       CREATED         SIZE
# stocktrader   latest    abc123def456   2 hours ago     1.7GB
```

## 第二步：选择使用方式

构建镜像后，你可以选择两种路径：

### 路径 A：本地运行 (docker run)

**用途**：在本地直接运行容器

#### 基本用法

```bash
docker run stocktrader:latest
```

#### 常用参数

```bash
docker run [选项] stocktrader:latest [命令]

# 常用选项：
# --rm         容器停止后自动删除
# -it          交互式终端
# -v           挂载目录（卷）
# -e           设置环境变量
# -d           后台运行
```

#### 开发模式（推荐）

代码通过卷挂载，修改后立即生效：

```bash
# Windows
docker run --rm -it ^
  -v "%cd%:/app" ^
  -v "%cd%/data:/app/data" ^
  -v "%cd%/logs:/app/logs" ^
  -v "%cd%/output:/app/output" ^
  -e PYTHONUNBUFFERED=1 ^
  -e PYTHONPATH=/app ^
  stocktrader:latest

# Linux/Mac
docker run --rm -it \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/output:/app/output" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest
```

**使用脚本**：
```bash
# Windows
docker-run-dev.bat

# Linux/Mac
./docker-run-dev.sh
```

#### 生产模式

代码已打包在镜像中：

```bash
# Windows
docker run --rm ^
  -v "%cd%/data:/app/data" ^
  -v "%cd%/logs:/app/logs" ^
  -v "%cd%/output:/app/output" ^
  -e PYTHONUNBUFFERED=1 ^
  -e PYTHONPATH=/app ^
  stocktrader:latest

# Linux/Mac
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/output:/app/output" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest
```

**使用脚本**：
```bash
# Windows
docker-run-prod.bat

# Linux/Mac
./docker-run-prod.sh
```

### 路径 B：导出镜像 (docker save)

**用途**：将镜像打包成文件，用于传输、备份或部署到其他机器

#### 导出镜像

```bash
docker save -o stocktrader-latest.tar stocktrader:latest
```

**参数解释**：
- `docker save`：导出镜像的命令
- `-o stocktrader-latest.tar`：输出文件名
- `stocktrader:latest`：要导出的镜像名称和标签

#### 导出后的使用

1. **传输到其他机器**（如 NAS）：
   ```bash
   # 复制文件到 NAS
   cp stocktrader-latest.tar /path/to/nas/
   ```

2. **在其他机器上导入**：
   ```bash
   # 在 NAS 上导入镜像
   docker load -i stocktrader-latest.tar
   
   # 验证镜像
   docker images stocktrader
   
   # 运行容器
   docker run stocktrader:latest
   ```

## Docker Compose：简化 docker run

### 什么是 Docker Compose？

Docker Compose 是一个工具，用于定义和运行多容器 Docker 应用程序。它使用 YAML 文件来配置服务，简化复杂的 `docker run` 命令。

**简单理解**：
- `docker run` = 运行单个容器（命令很长）
- `docker-compose` = 管理容器（一个命令搞定）

### 为什么使用 Docker Compose？

#### 对比：docker run vs docker-compose

**不使用 Compose（复杂）**：
```bash
docker run --rm -it \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/output:/app/output" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest
```

**使用 Compose（简单）**：
```bash
docker-compose up
```

### docker-compose.yml 配置

项目中的 `docker-compose.yml`：

```yaml
version: '3.8'

services:
  stocktrader:
    build: .                    # 从 Dockerfile 构建镜像
    container_name: stocktrader  # 容器名称
    
    volumes:                    # 挂载目录（相当于 -v 参数）
      - .:/app                 # 挂载代码目录
      - ./data:/app/data       # 挂载数据目录
      - ./logs:/app/logs       # 挂载日志目录
      - ./output:/app/output   # 挂载输出目录
    
    environment:                # 环境变量（相当于 -e 参数）
      - PYTHONUNBUFFERED=1
      - PYTHONPATH=/app
      # - TUSHARE_TOKEN=${TUSHARE_TOKEN}  # 从 .env 文件读取
```

### Docker Compose 常用命令

#### 基本操作

```bash
# 启动服务（前台运行，可以看到日志）
docker-compose up

# 启动服务（后台运行）
docker-compose up -d

# 停止服务
docker-compose down

# 查看运行状态
docker-compose ps

# 查看日志
docker-compose logs

# 查看实时日志
docker-compose logs -f
```

#### 构建和重建

```bash
# 构建镜像并启动
docker-compose up --build

# 只构建镜像，不启动
docker-compose build

# 强制重新构建（不使用缓存）
docker-compose build --no-cache
```

#### 执行命令

```bash
# 在运行中的容器中执行命令
docker-compose exec stocktrader uv run python main.py

# 运行一次性命令（会创建新容器）
docker-compose run stocktrader uv run python scripts/fetch_kline_data.py

# 进入容器
docker-compose exec stocktrader /bin/bash
```

### Docker Compose 使用场景

#### 场景 1：开发模式

```bash
# 启动服务（代码自动挂载，修改立即生效）
docker-compose up

# 修改代码后，重启服务
docker-compose restart

# 或者重新构建并启动
docker-compose up --build
```

#### 场景 2：运行脚本

```bash
# 运行数据获取脚本
docker-compose run stocktrader uv run python scripts/fetch_kline_data.py

# 运行策略脚本
docker-compose run stocktrader uv run python scripts/run_strategies.py
```

#### 场景 3：调试

```bash
# 进入容器
docker-compose exec stocktrader /bin/bash

# 在容器内执行命令
docker-compose exec stocktrader uv run python -c "import pandas; print(pandas.__version__)"
```

## 完整使用流程

### 场景 1：本地开发

```bash
# 方式 1：使用 docker run
docker build -t stocktrader:latest .
docker-run-dev.bat  # 或手动运行 docker run 命令

# 方式 2：使用 docker-compose（推荐）
docker-compose up
```

### 场景 2：部署到 NAS

```bash
# 1. 构建镜像
docker build -t stocktrader:latest .

# 2. 导出镜像
docker save -o stocktrader-latest.tar stocktrader:latest

# 3. 传输到 NAS（通过文件共享或 SCP）
cp stocktrader-latest.tar /path/to/nas/

# 4. 在 NAS 上导入
docker load -i stocktrader-latest.tar

# 5. 在 NAS 上运行
docker run -d \
  --name stocktrader \
  -v /path/to/data:/app/data \
  -v /path/to/logs:/app/logs \
  stocktrader:latest
```

### 场景 3：生产环境

```bash
# 1. 构建镜像（包含所有代码）
docker build -t stocktrader:latest .

# 2. 运行容器（代码已在镜像中）
docker-run-prod.bat  # 或手动运行 docker run 命令

# 或使用 docker-compose（需要修改配置，不挂载代码目录）
docker-compose up
```

## 目录挂载说明

### 挂载的作用

- **代码目录** (`-v .:/app`)：开发模式时挂载，代码修改立即生效
- **数据目录** (`-v ./data:/app/data`)：持久化数据库文件
- **日志目录** (`-v ./logs:/app/logs`)：持久化日志文件
- **输出目录** (`-v ./output:/app/output`)：持久化输出文件

### 开发模式 vs 生产模式

| 模式 | 代码挂载 | 特点 |
|------|---------|------|
| **开发模式** | ✅ 挂载代码目录 | 代码修改立即生效，无需重新构建 |
| **生产模式** | ❌ 不挂载代码 | 代码打包在镜像中，更稳定 |

## 镜像存储位置

### 构建后的镜像存储

- **Windows (Docker Desktop)**：存储在 WSL2 虚拟硬盘中
  - 路径：`C:\ProgramData\Docker\wsl\data\ext4.vhdx`
  - 不直接访问，由 Docker 管理

- **Linux**：存储在 `/var/lib/docker/`

- **Mac (Docker Desktop)**：存储在虚拟机磁盘文件中

### 查看镜像

```bash
# 查看所有本地镜像
docker images

# 查看镜像详细信息
docker inspect stocktrader:latest
```

### 导出镜像

```bash
# 导出镜像为 tar 文件
docker save -o stocktrader-latest.tar stocktrader:latest

# tar 文件存储在文件系统中，可以复制、传输
```

## 环境变量管理

### 方式 1：命令行传递

```bash
docker run -e TUSHARE_TOKEN=your_token stocktrader:latest
```

### 方式 2：使用 .env 文件

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

## 常见问题

### Q: docker build 之后镜像在哪里？

A: 镜像存储在 Docker 的本地存储中（Windows 在 WSL2 虚拟硬盘）。使用 `docker images` 查看，使用 `docker save` 导出为文件。

### Q: docker save 是把哪里的镜像生成 tar 文件？

A: 从 Docker 的本地存储中读取镜像，打包成 tar 文件。tar 文件可以传输到其他机器，使用 `docker load` 导入。

### Q: 代码修改后不生效？

A: 
- 开发模式：确保使用了卷挂载 `-v .:/app`
- 生产模式：需要重新构建镜像 `docker build -t stocktrader:latest .`

### Q: 如何查看容器中的代码？

A: 
```bash
# 进入运行中的容器
docker exec -it <container_id> /bin/bash

# 或使用 docker-compose
docker-compose exec stocktrader /bin/bash
```

### Q: 如何更新依赖？

A: 修改 `pyproject.toml` 后，重新构建镜像：
```bash
docker build -t stocktrader:latest .
```

### Q: docker run 和 docker-compose 有什么区别？

A: 
- `docker run`：直接运行容器，命令较长
- `docker-compose`：通过配置文件管理容器，命令简单，适合复杂配置

### Q: 什么时候用 docker run，什么时候用 docker-compose？

A: 
- **docker run**：简单场景，单次运行，快速测试
- **docker-compose**：开发环境，复杂配置，多服务，团队协作

## 快速参考

### 基本流程

```bash
# 1. 构建镜像
docker build -t stocktrader:latest .

# 2. 选择路径
# 路径 A：本地运行
docker run stocktrader:latest
# 或使用 docker-compose
docker-compose up

# 路径 B：导出镜像
docker save -o stocktrader-latest.tar stocktrader:latest
```

### 常用命令

```bash
# 镜像操作
docker images                    # 查看镜像
docker build -t name:tag .      # 构建镜像
docker save -o file.tar name:tag # 导出镜像
docker load -i file.tar         # 导入镜像

# 容器操作
docker run [选项] image          # 运行容器
docker ps                        # 查看运行中的容器
docker exec -it container bash  # 进入容器

# Docker Compose
docker-compose up                # 启动服务
docker-compose down              # 停止服务
docker-compose logs -f           # 查看日志
docker-compose exec service bash # 进入容器
```

## 总结

Docker 的核心流程就是：

1. **构建** (`docker build`)：创建镜像
2. **运行** (`docker run` 或 `docker-compose`)：本地使用
3. **导出** (`docker save`)：传输到其他机器

**Docker Compose** 是 `docker run` 的简化版本，通过配置文件管理容器，让命令更简单、配置更清晰。
