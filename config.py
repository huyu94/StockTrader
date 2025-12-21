import os
import sys
from datetime import datetime
from loguru import logger
from project_var import PROJECT_DIR

def setup_logger(level_console: str = "INFO", level_file: str = "DEBUG", file_pattern: str = None):
    logs_dir = os.path.join(PROJECT_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    if not file_pattern:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_pattern = f"run_{ts}.log"
    file_path = file_pattern if os.path.isabs(file_pattern) else os.path.join(logs_dir, file_pattern)
    logger.remove()
    logger.add(sys.stdout, level=level_console)
    logger.add(file_path, level=level_file, encoding="utf-8")
    return logger

