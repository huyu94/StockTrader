from dotenv import load_dotenv
import os

# 加载.env文件
load_dotenv()

# Tushare API配置
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "your_tushare_token_here")  # 从环境变量获取API密钥
print(f"TUSHARE_TOKEN: {TUSHARE_TOKEN}")
# 数据存储路径
DATA_PATH = os.getenv("DATA_PATH", "data/")

# 日志配置
LOG_LEVEL = "INFO"