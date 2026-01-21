from loguru import logger

from backend.src.services.settings import Settings


def init_logging() -> None:
    settings = Settings.from_env()
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=settings.log_level,
        backtrace=False,
        diagnose=False,
    )
