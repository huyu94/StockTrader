# Docker 镜像构建和 NAS 部署指南

## 问题：网络连接失败

如果遇到以下错误：
```
ERROR: failed to solve: python:3.12.8-slim: failed to resolve source metadata
```

这是因为无法连接到 Docker Hub。需要配置镜像加速器。

## 解决方案

### 方案 1：配置 Docker 镜像加速器（推荐）

#### Windows Docker Desktop

1. 打开 Docker Desktop
2. 点击右上角 **设置** (Settings)
3. 选择 **Docker Engine**
4. 在 JSON 配置中添加：

```json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.baidubce.com"
  ]
}
```

5. 点击 **Apply & Restart**

#### 或者使用阿里云镜像加速器

1. 登录 [阿里云容器镜像服务](https://cr.console.aliyun.com/)
2. 获取你的专属加速地址（格式：`https://xxxxx.mirror.aliyuncs.com`）
3. 添加到 Docker Engine 配置中

### 方案 2：使用国内镜像源（临时方案）

如果无法配置镜像加速器，可以修改 Dockerfile 使用国内镜像：

```dockerfile
# 使用国内镜像源
FROM registry.cn-hangzhou.aliyuncs.com/acs/python:3.12.8-slim
```

或者使用其他国内镜像源。

## 构建和导出镜像

### 方法 1：使用脚本（推荐）

```cmd
REM Windows
docker-build-export.bat
```

脚本会自动：
1. 构建镜像
2. 导出为 tar 文件
3. 显示文件信息

### 方法 2：手动操作

```cmd
REM 1. 构建镜像
docker build -t stocktrader:latest .

REM 2. 导出镜像为 tar 文件
docker save -o stocktrader-latest.tar stocktrader:latest
```

## 传输到 NAS

### 方法 1：通过文件共享（推荐）

1. **在 Windows 上**：
   - 找到生成的 `stocktrader-latest.tar` 文件
   - 复制到 NAS 的共享文件夹（如 `\\192.168.1.100\docker\`）

2. **在 NAS 上**：
   - 通过 SSH 或 NAS 的文件管理器访问
   - 找到传输的文件

### 方法 2：通过 SCP（如果 NAS 支持 SSH）

```bash
# 在 Windows 上使用 PowerShell 或 Git Bash
scp stocktrader-latest.tar user@nas-ip:/path/to/docker/
```

### 方法 3：通过 NAS Web 界面

1. 登录 NAS 的 Web 管理界面
2. 使用文件管理器上传 `stocktrader-latest.tar`

## 在 NAS 上导入镜像

### 方法 1：通过 SSH（推荐）

```bash
# 1. SSH 连接到 NAS
ssh user@nas-ip

# 2. 进入文件所在目录
cd /path/to/stocktrader-latest.tar

# 3. 导入镜像
docker load -i stocktrader-latest.tar

# 4. 验证镜像
docker images | grep stocktrader
```

### 方法 2：通过 NAS Docker 管理界面

如果 NAS 有 Docker 图形界面（如群晖 DSM、威联通 QTS）：
1. 打开 Docker 管理界面
2. 选择 **镜像** → **导入**
3. 选择 `stocktrader-latest.tar` 文件
4. 等待导入完成

### 方法 3：通过 NAS 命令行

```bash
# 在 NAS 的终端中执行
docker load < /path/to/stocktrader-latest.tar
```

## 在 NAS 上运行容器

### 基本运行

```bash
docker run --rm \
  -v /path/to/data:/app/data \
  -v /path/to/logs:/app/logs \
  -v /path/to/output:/app/output \
  -e TUSHARE_TOKEN=your_token \
  stocktrader:latest
```

### 使用 docker-compose（推荐）

在 NAS 上创建 `docker-compose.yml`：

```yaml
version: '3.8'

services:
  stocktrader:
    image: stocktrader:latest
    container_name: stocktrader
    volumes:
      - /nas/data/stocktrader/data:/app/data
      - /nas/data/stocktrader/logs:/app/logs
      - /nas/data/stocktrader/output:/app/output
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONPATH=/app
      - TUSHARE_TOKEN=${TUSHARE_TOKEN}
    restart: unless-stopped
```

然后运行：
```bash
docker-compose up -d
```

## 完整流程示例

### Windows 端操作

```cmd
REM 1. 配置 Docker 镜像加速器（如果还没配置）
REM 打开 Docker Desktop -> Settings -> Docker Engine -> 添加镜像地址

REM 2. 构建并导出镜像
docker-build-export.bat

REM 3. 复制文件到 NAS 共享文件夹
copy stocktrader-latest.tar \\192.168.1.100\docker\
```

### NAS 端操作（SSH）

```bash
# 1. SSH 连接到 NAS
ssh admin@192.168.1.100

# 2. 进入 Docker 目录
cd /volume1/docker

# 3. 导入镜像
docker load -i stocktrader-latest.tar

# 4. 验证
docker images

# 5. 运行容器
docker run -d \
  --name stocktrader \
  -v /volume1/docker/stocktrader/data:/app/data \
  -v /volume1/docker/stocktrader/logs:/app/logs \
  -e TUSHARE_TOKEN=your_token \
  stocktrader:latest
```

## 常见问题

### Q: 镜像文件太大怎么办？

A: 可以压缩后再传输：
```bash
# 压缩
gzip stocktrader-latest.tar

# 传输 stocktrader-latest.tar.gz

# 在 NAS 上解压
gunzip stocktrader-latest.tar.gz
docker load -i stocktrader-latest.tar
```

### Q: 如何更新镜像？

A: 在 Windows 上重新构建并导出，然后替换 NAS 上的镜像：
```bash
# NAS 上先删除旧镜像
docker rmi stocktrader:latest

# 导入新镜像
docker load -i stocktrader-latest.tar
```

### Q: NAS 上如何查看容器日志？

A:
```bash
# 查看日志
docker logs stocktrader

# 实时查看日志
docker logs -f stocktrader
```

### Q: 如何设置自动启动？

A: 在 docker-compose.yml 中添加：
```yaml
restart: unless-stopped
```

或者在 docker run 中添加：
```bash
docker run --restart unless-stopped ...
```

## 镜像大小优化建议

如果镜像太大，可以考虑：

1. **使用多阶段构建**：减少最终镜像大小
2. **清理缓存**：在 Dockerfile 中清理 apt 缓存
3. **使用 .dockerignore**：排除不必要的文件

当前的 Dockerfile 已经包含了这些优化。

