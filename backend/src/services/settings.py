import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str
    database_url: str
    log_level: str

    @staticmethod
    def _build_database_url() -> str:
        direct_url = os.getenv("DATABASE_URL")
        if direct_url:
            return direct_url

        mysql_host = os.getenv("MYSQL_HOST")
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_db = os.getenv("MYSQL_DATABASE")
        mysql_port = os.getenv("MYSQL_PORT", "3306")
        mysql_charset = os.getenv("MYSQL_CHARSET", "utf8mb4")

        if mysql_host and mysql_user and mysql_password and mysql_db:
            return (
                f"mysql+pymysql://{mysql_user}:{mysql_password}"
                f"@{mysql_host}:{mysql_port}/{mysql_db}?charset={mysql_charset}"
            )

        default_sqlite = os.getenv("SQLITE_PATH", "data/stock_data.db")
        return f"sqlite:///{default_sqlite}"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_name=os.getenv("APP_NAME", "StockTrader Dashboard"),
            database_url=cls._build_database_url(),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
