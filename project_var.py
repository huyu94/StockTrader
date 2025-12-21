#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目变量定义
统一管理项目中使用的各种路径和常量
"""

import os

# 获取当前文件的绝对路径
CURRENT_FILE = os.path.abspath(__file__)
# 获取项目根目录
PROJECT_DIR = os.path.dirname(CURRENT_FILE)

# 数据存储目录
DATA_DIR = os.path.join(PROJECT_DIR, "data")

# 缓存目录
CACHE_DIR = os.path.join(PROJECT_DIR, "cache")

# 输出目录
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")

# 确保所有目录存在
for dir_path in [DATA_DIR, CACHE_DIR, OUTPUT_DIR]:
    os.makedirs(dir_path, exist_ok=True)