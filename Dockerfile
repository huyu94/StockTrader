# 使用 Python 3.12.8 作为基础镜像
FROM python:3.12.8-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖（包括构建工具）
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 安装 uv
RUN pip install --no-cache-dir uv

# 复制项目依赖文件
COPY pyproject.toml uv.lock ./

# 使用 uv 安装依赖
RUN uv sync --frozen

# 移除构建工具（节省空间）
RUN apt-get purge -y build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY . .

# 创建必需的目录
RUN mkdir -p /app/data /app/logs /app/output

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    TZ=Asia/Shanghai

# 默认命令
CMD ["uv", "run", "python", "main.py"]