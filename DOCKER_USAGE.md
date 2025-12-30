# Docker 使用指南

## 代码如何进入容器

### 方式 1：构建时复制（生产模式）

**特点**：代码被打包到镜像中，镜像自包含，适合生产环境。

```bash
# 构建镜像（代码会在构建时复制到镜像中）
docker build -t stocktrader:latest .

# 运行容器
docker run --rm stocktrader:latest
```

**Dockerfile 中的关键步骤**：
```dockerfile
# 复制项目文件（第 23 行）
COPY . .
```

### 方式 2：运行时挂载（开发模式，推荐）

**特点**：代码通过卷挂载，修改代码后立即生效，无需重新构建镜像。

#### Windows 使用方式：

```cmd
REM 使用批处理脚本
docker-run-dev.bat

REM 或者手动运行
docker run --rm -it -v "%cd%:/app" stocktrader:latest
```

#### Linux/Mac 使用方式：

```bash
# 使用 shell 脚本
chmod +x docker-run-dev.sh
./docker-run-dev.sh

# 或者手动运行
docker run --rm -it -v "$(pwd):/app" stocktrader:latest
```

#### 使用 docker-compose：

```bash
# 启动服务（代码会自动挂载）
docker-compose up

# 后台运行
docker-compose up -d

# 停止服务
docker-compose down
```

### 方式 3：进入容器查看代码

如果你想查看容器中的代码：

```bash
# 进入运行中的容器
docker exec -it <container_id> /bin/bash

# 或者启动一个交互式容器
docker run --rm -it stocktrader:latest /bin/bash

# 查看代码
ls -la /app
cat /app/main.py
```

## 完整使用示例

### 开发模式（推荐）

```bash
# 1. 构建镜像（只需要构建一次，或依赖变化时重新构建）
docker build -t stocktrader:latest .

# 2. 运行容器，挂载代码目录
docker run --rm -it \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -e TUSHARE_TOKEN=your_token \
  stocktrader:latest

# 3. 修改代码后，直接重新运行容器即可，无需重新构建
```

### 生产模式

```bash
# 1. 构建镜像（包含所有代码）
docker build -t stocktrader:latest .

# 2. 运行容器（代码已在镜像中）
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -e TUSHARE_TOKEN=your_token \
  stocktrader:latest
```

## 目录挂载说明

- **代码目录** (`-v .:/app`)：开发模式时挂载，代码修改立即生效
- **数据目录** (`-v ./data:/app/data`)：持久化数据库文件
- **日志目录** (`-v ./logs:/app/logs`)：持久化日志文件
- **输出目录** (`-v ./output:/app/output`)：持久化输出文件

## 常见问题

### Q: 代码修改后不生效？
A: 确保使用了卷挂载 `-v .:/app`，或者重新构建镜像。

### Q: 如何查看容器中的代码？
A: 使用 `docker exec -it <container_id> /bin/bash` 进入容器，然后 `ls /app` 查看。

### Q: 如何更新依赖？
A: 修改 `pyproject.toml` 后，重新构建镜像：`docker build -t stocktrader:latest .`

### Q: 如何传递环境变量？
A: 使用 `-e` 参数：`docker run -e TUSHARE_TOKEN=xxx stocktrader:latest`

