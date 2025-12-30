#!/bin/bash
# 生产模式运行脚本 - 代码已打包在镜像中

# 构建镜像（包含所有代码）
docker build -t stocktrader:latest .

# 运行容器，只挂载数据目录（代码已在镜像中）
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/output:/app/output" \
  -e PYTHONUNBUFFERED=1 \
  -e PYTHONPATH=/app \
  stocktrader:latest

