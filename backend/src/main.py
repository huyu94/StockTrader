from fastapi import FastAPI

from backend.src.api.router import api_router
from backend.src.services.logging import init_logging


def create_app() -> FastAPI:
    init_logging()
    app = FastAPI(title="StockTrader Web Dashboard", version="0.1.0")
    app.include_router(api_router, prefix="/api")
    return app


app = create_app()
