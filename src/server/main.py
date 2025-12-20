#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCP Server主文件
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import uvicorn
import os
import sys
from loguru import logger

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.server.config import config
from src.server.endpoints import indicator

# 配置日志
logger.add(
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs', 'mcp_server.log'),
    rotation='daily',
    level=config.LOG_LEVEL,
    encoding='utf-8'
)

# 创建限流器
limiter = Limiter(key_func=get_remote_address, default_limits=[config.RATE_LIMIT])

# 创建FastAPI应用
app = FastAPI(
    title="Stock Indicator MCP Server",
    description="股票指标计算MCP Server，提供个股指标数据的API接口",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该设置具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 添加限流器
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# 注册路由
app.include_router(
    indicator.router,
    prefix=f"{config.API_V1_STR}/indicator",
    tags=["indicator"]
)


@app.get("/")
async def root():
    """
    根路径
    """
    return {
        "message": "Stock Indicator MCP Server",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health")
async def health_check():
    """
    健康检查
    """
    return {"status": "healthy"}


if __name__ == "__main__":
    logger.info(f"启动MCP Server，监听地址：{config.HOST}:{config.PORT}")
    uvicorn.run(
        "src.server.main:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.DEBUG,
        log_level=config.LOG_LEVEL.lower()
    )
