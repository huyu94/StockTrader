#!/bin/bash
# 开发模式运行脚本 - 使用卷挂载，代码修改立即生效

# 构建镜像（如果还没有构建）
docker build -t stocktrader:latest .

# 运行容器，挂载代码目录
docker run --rm -it \
  -v "$(pwd):/app" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/output:/app/output" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest \
  "$@"

