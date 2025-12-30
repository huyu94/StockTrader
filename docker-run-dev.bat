@echo off
REM 开发模式运行脚本 - 使用卷挂载，代码修改立即生效

REM 构建镜像（如果还没有构建）
docker build -t stocktrader:latest .

REM 运行容器，挂载代码目录
docker run --rm -it ^
  -v "%cd%:/app" ^
  -v "%cd%/data:/app/data" ^
  -v "%cd%/logs:/app/logs" ^
  -v "%cd%/output:/app/output" ^
  -e PYTHONUNBUFFERED=1 ^
  -e PYTHONPATH=/app ^
  stocktrader:latest %*

