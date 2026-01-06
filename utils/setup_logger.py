import os
import sys
from datetime import datetime
from loguru import logger
from project_var import PROJECT_DIR

def setup_logger(
    level_console: str = "INFO", 
    level_file: str = "DEBUG", 
    file_pattern: str = None,
    rotation: str = "100 MB",  # 日志文件大小达到100MB时轮转
    retention: str = "30 days"  # 保留30天的日志文件
):
    """
    设置日志记录器
    
    Args:
        level_console: 控制台日志级别，默认 "INFO"
        level_file: 文件日志级别，默认 "DEBUG"
        file_pattern: 日志文件名模式，如果为None则使用时间戳生成
        rotation: 日志轮转条件，可以是：
            - 文件大小：如 "100 MB", "500 MB", "1 GB"
            - 时间：如 "1 day", "1 week", "midnight"
            - 文件数量：如 "100"
            默认 "100 MB"
        retention: 日志保留时间或数量，可以是：
            - 时间：如 "7 days", "1 month", "1 year"
            - 文件数量：如 "10" 表示保留10个文件
            默认 "30 days"
    
    Returns:
        logger: 配置好的日志记录器
    """
    logs_dir = os.path.join(PROJECT_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    if not file_pattern:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_pattern = f"run_{ts}.log"
    file_path = file_pattern if os.path.isabs(file_pattern) else os.path.join(logs_dir, file_pattern)
    logger.remove()
    logger.add(sys.stdout, level=level_console)
    logger.add(
        file_path, 
        level=level_file, 
        encoding="utf-8",
        rotation=rotation,  # 日志轮转
        retention=retention,  # 日志保留
        compression="zip"  # 压缩旧日志文件
    )
    return logger

