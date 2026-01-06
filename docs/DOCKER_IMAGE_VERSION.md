# Docker 镜像版本管理

## 如何设置镜像版本

### 方式 1：通过环境变量设置（推荐）

在 `.env` 文件中设置 `IMAGE_VERSION`：

```env
# 在 .env 文件中
IMAGE_VERSION=v1.0.0
```

然后构建和运行：

```bash
docker-compose build
docker-compose up -d
```

构建的镜像名称将是：`stocktrader:v1.0.0`

### 方式 2：在命令行中指定

```bash
# 构建时指定版本
IMAGE_VERSION=v1.0.0 docker-compose build

# 或者使用 export（Linux/Mac）
export IMAGE_VERSION=v1.0.0
docker-compose build

# Windows CMD
set IMAGE_VERSION=v1.0.0
docker-compose build

# Windows PowerShell
$env:IMAGE_VERSION="v1.0.0"
docker-compose build
```

### 方式 3：直接使用 docker build 命令

```bash
# 构建并指定标签
docker build -t stocktrader:v1.0.0 .
docker build -t stocktrader:latest .

# 构建多个标签
docker build -t stocktrader:v1.0.0 -t stocktrader:latest .
```

## 版本命名建议

### 语义化版本（推荐）

```
v1.0.0        # 主版本.次版本.修订版本
v1.0.1        # 小更新
v1.1.0        # 新功能
v2.0.0        # 重大更新
```

### 日期版本

```
20260106      # 年月日
2026-01-06    # 带分隔符
20260106.1    # 日期 + 序号
```

### 其他格式

```
latest        # 最新版本（默认）
dev           # 开发版本
prod          # 生产版本
stable        # 稳定版本
```

## 查看镜像版本

### 查看所有镜像

```bash
# 查看所有 stocktrader 镜像
docker images | grep stocktrader

# 输出示例：
# REPOSITORY    TAG       IMAGE ID       CREATED         SIZE
# stocktrader   v1.0.0    abc123def456   2 hours ago     1.2GB
# stocktrader   latest    abc123def456   2 hours ago     1.2GB
```

### 查看镜像详细信息

```bash
# 查看镜像详细信息
docker inspect stocktrader:v1.0.0

# 查看镜像历史
docker history stocktrader:v1.0.0
```

## 使用特定版本的镜像

### 在 docker-compose.yml 中

当前配置已经支持通过环境变量指定版本：

```yaml
image: stocktrader:${IMAGE_VERSION:-latest}
```

如果不设置 `IMAGE_VERSION`，默认使用 `latest`。

### 直接使用 docker run

```bash
# 运行特定版本
docker run -d --name stocktrader stocktrader:v1.0.0

# 运行最新版本
docker run -d --name stocktrader stocktrader:latest
```

## 版本管理最佳实践

### 1. 构建时同时打多个标签

```bash
# 构建时同时标记为版本号和 latest
docker build -t stocktrader:v1.0.0 -t stocktrader:latest .
```

### 2. 保留历史版本

```bash
# 不要删除旧版本，保留用于回滚
docker images stocktrader

# 只删除特定版本
docker rmi stocktrader:old-version

# 清理未使用的镜像（谨慎使用）
docker image prune
```

### 3. 导出特定版本

```bash
# 导出特定版本的镜像
docker save -o stocktrader-v1.0.0.tar stocktrader:v1.0.0

# 导出多个版本
docker save -o stocktrader-images.tar stocktrader:v1.0.0 stocktrader:latest
```

### 4. 版本回滚

如果需要回滚到之前的版本：

```bash
# 1. 停止当前容器
docker-compose down

# 2. 修改 .env 文件中的 IMAGE_VERSION
# IMAGE_VERSION=v0.9.0

# 3. 启动旧版本
docker-compose up -d
```

## 实际使用示例

### 场景 1：开发新功能

```bash
# 1. 开发完成后，设置版本号
echo "IMAGE_VERSION=v1.1.0" >> .env

# 2. 构建新版本
docker-compose build

# 3. 测试新版本
docker-compose up -d

# 4. 验证无误后，标记为 latest
docker tag stocktrader:v1.1.0 stocktrader:latest
```

### 场景 2：发布到生产环境

```bash
# 1. 构建生产版本
IMAGE_VERSION=v1.0.0 docker-compose build

# 2. 导出镜像文件
docker save -o stocktrader-v1.0.0.tar stocktrader:v1.0.0

# 3. 传输到 NAS 服务器
# scp stocktrader-v1.0.0.tar user@nas:/path/to/

# 4. 在 NAS 上导入
docker load -i stocktrader-v1.0.0.tar

# 5. 在 NAS 上运行
docker run -d --name stocktrader --env-file .env stocktrader:v1.0.0
```

### 场景 3：版本升级

```bash
# 1. 备份当前版本
docker tag stocktrader:latest stocktrader:v0.9.0-backup

# 2. 构建新版本
IMAGE_VERSION=v1.0.0 docker-compose build

# 3. 测试新版本
docker-compose up -d

# 4. 如果出现问题，快速回滚
docker-compose down
docker tag stocktrader:v0.9.0-backup stocktrader:latest
docker-compose up -d
```

## 当前配置说明

在 `docker-compose.yml` 中，镜像版本配置如下：

```yaml
build:
  context: .
  tags:
    - stocktrader:latest
    - stocktrader:${IMAGE_VERSION:-latest}
image: stocktrader:${IMAGE_VERSION:-latest}
```

**说明**：
- `tags`：构建时会同时创建两个标签（latest 和指定版本）
- `image`：运行时会使用指定版本的镜像
- `${IMAGE_VERSION:-latest}`：如果未设置 `IMAGE_VERSION`，默认使用 `latest`

## 快速参考

```bash
# 设置版本并构建
IMAGE_VERSION=v1.0.0 docker-compose build

# 查看所有版本
docker images stocktrader

# 运行特定版本
IMAGE_VERSION=v1.0.0 docker-compose up -d

# 导出镜像
docker save -o stocktrader-v1.0.0.tar stocktrader:v1.0.0

# 导入镜像
docker load -i stocktrader-v1.0.0.tar
```

