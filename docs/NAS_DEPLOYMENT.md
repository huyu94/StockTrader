# NAS 部署指南

## 前置准备

### 1. 准备环境变量文件

复制环境变量模板文件并编辑：

```bash
# Windows
copy env.example .env

# Linux/Mac
cp env.example .env
```

编辑 `.env` 文件，填入实际配置：

```env
# MySQL 数据库配置
# 如果 MySQL 在 NAS 上运行，MYSQL_HOST 应该是 NAS 的 IP 地址
MYSQL_HOST=192.168.1.105
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=stock_test
MYSQL_CHARSET=utf8mb4

# Tushare API Token（必需）
TUSHARE_TOKEN=your_tushare_token_here

# 消息机器人 Access Token（可选）
MESSAGE_ROBOT_ACCESS_TOKEN=your_message_robot_token_here
```

## 方式一：使用 Docker Compose（推荐）

### 1. 构建镜像并启动

```bash
# 构建镜像并启动容器（后台运行）
docker-compose up -d --build

# 或者分步执行
# 1. 先构建镜像
docker-compose build

# 2. 再启动容器
docker-compose up -d
```

### 2. 查看运行状态

```bash
# 查看容器状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 查看最近 100 行日志
docker-compose logs --tail=100
```

### 3. 停止和重启

```bash
# 停止容器
docker-compose down

# 重启容器
docker-compose restart

# 停止并删除容器（保留镜像）
docker-compose down

# 停止并删除容器和镜像
docker-compose down --rmi local
```

## 方式二：使用 Docker 命令

### 1. 构建镜像

```bash
# 构建镜像（标签为 stocktrader:latest）
docker build -t stocktrader:latest .

# 查看构建的镜像
docker images | grep stocktrader
```

### 2. 运行容器

```bash
# 从 .env 文件加载环境变量并运行
docker run -d \
  --name stocktrader \
  --restart always \
  --env-file .env \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  -e TZ=Asia/Shanghai \
  stocktrader:latest
```

### 3. 查看和管理

```bash
# 查看运行中的容器
docker ps | grep stocktrader

# 查看日志
docker logs -f stocktrader

# 停止容器
docker stop stocktrader

# 启动容器
docker start stocktrader

# 删除容器
docker rm stocktrader
```

## 导出和导入镜像文件

### 导出镜像为文件

```bash
# 导出镜像为 tar 文件
docker save -o stocktrader-latest.tar stocktrader:latest

# 或者使用 gzip 压缩（文件更小）
docker save stocktrader:latest | gzip > stocktrader-latest.tar.gz
```

### 导入镜像文件

```bash
# 从 tar 文件导入镜像
docker load -i stocktrader-latest.tar

# 从压缩文件导入
gunzip -c stocktrader-latest.tar.gz | docker load
```

### 在 NAS 上导入镜像

1. 将导出的镜像文件传输到 NAS
2. 在 NAS 上执行导入命令
3. 使用 docker-compose 或 docker run 启动容器

## 验证部署

### 1. 检查容器是否运行

```bash
docker-compose ps
# 或
docker ps | grep stocktrader
```

### 2. 查看应用日志

```bash
docker-compose logs -f stocktrader
# 或
docker logs -f stocktrader
```

应该能看到类似以下日志：
```
定时任务调度器已启动
任务1: 每日数据更新
  调度时间: 每天19:00
任务2: 策略流水线
  调度时间: 每天14:00（仅交易日执行）
```

### 3. 进入容器检查

```bash
# 进入容器
docker-compose exec stocktrader /bin/bash
# 或
docker exec -it stocktrader /bin/bash

# 检查目录是否存在
ls -la /app/data /app/logs /app/output

# 检查环境变量
env | grep MYSQL
env | grep TUSHARE
```

## 常见问题

### 1. 容器启动后立即退出

**原因**：可能是环境变量配置错误或 MySQL 连接失败

**解决**：
```bash
# 查看详细日志
docker-compose logs stocktrader

# 检查 .env 文件配置是否正确
cat .env
```

### 2. 无法连接 MySQL

**检查**：
- `MYSQL_HOST` 是否正确（应该是 NAS 的 IP 地址）
- MySQL 服务是否在运行
- 防火墙是否开放 3306 端口
- MySQL 用户权限是否正确

### 3. 时区不正确

容器已配置 `TZ=Asia/Shanghai`，如果还有问题，检查：
```bash
docker-compose exec stocktrader date
```

## 更新应用

### 方式一：使用 Docker Compose

```bash
# 1. 停止容器
docker-compose down

# 2. 重新构建镜像（代码更新后）
docker-compose build

# 3. 启动容器
docker-compose up -d
```

### 方式二：使用 Docker 命令

```bash
# 1. 停止并删除旧容器
docker stop stocktrader
docker rm stocktrader

# 2. 重新构建镜像
docker build -t stocktrader:latest .

# 3. 启动新容器
docker run -d \
  --name stocktrader \
  --restart always \
  --env-file .env \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  -e TZ=Asia/Shanghai \
  stocktrader:latest
```

## 数据持久化说明

**注意**：当前配置中，`data`、`logs`、`output` 目录保存在容器内，**不会持久化**。

如果需要持久化数据，可以：

1. **使用 Docker Volume**（推荐）：
```yaml
volumes:
  - stocktrader_data:/app/data
  - stocktrader_logs:/app/logs
  - stocktrader_output:/app/output

volumes:
  stocktrader_data:
  stocktrader_logs:
  stocktrader_output:
```

2. **挂载到宿主机目录**：
```yaml
volumes:
  - ./data:/app/data
  - ./logs:/app/logs
  - ./output:/app/output
```

## 定时任务说明

应用启动后会自动运行以下定时任务：

1. **每日数据更新**：每天 19:00 执行
   - 更新股票基本信息
   - 更新交易日历
   - 更新日K线数据
   - 更新复权因子
   - 更新前复权数据

2. **策略流水线**：每天 14:00 执行（仅交易日）
   - 更新实时K线数据
   - 运行策略筛选
   - 输出结果到 CSV 文件
   - 发送通知（如果配置了消息机器人）

## 快速参考

```bash
# 一键启动（构建+运行）
docker-compose up -d --build

# 查看日志
docker-compose logs -f

# 停止
docker-compose down

# 重启
docker-compose restart

# 导出镜像
docker save -o stocktrader-latest.tar stocktrader:latest
```

