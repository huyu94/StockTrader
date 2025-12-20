#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCP Server配置文件
"""

from pydantic_settings import BaseSettings
from typing import Optional


class MCPConfig(BaseSettings):
    """
    MCP Server配置
    """
    # 服务器配置
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False
    
    # API配置
    API_V1_STR: str = "/api/v1"
    
    # 身份验证配置
    SECRET_KEY: str = "your-secret-key-here"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # 限流配置
    RATE_LIMIT: str = "100/minute"
    
    # 日志配置
    LOG_LEVEL: str = "INFO"
    
    class Config:
        """
        配置类
        """
        env_file = ".env"
        case_sensitive = True


# 创建配置实例
config = MCPConfig()
