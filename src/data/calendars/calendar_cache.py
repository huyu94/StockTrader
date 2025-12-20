import os
import pandas as pd
from project_var import PROJECT_DIR

def _cache_dir() -> str:
    path = os.path.join(PROJECT_DIR, "cache")
    os.makedirs(path, exist_ok=True)
    return path

def _cache_path(exchange: str = "SSE") -> str:
    return os.path.join(_cache_dir(), f"trade_calendar_{exchange}.csv")

def load(exchange: str = "SSE") -> pd.DataFrame:
    path = _cache_path(exchange)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save(exchange: str, df: pd.DataFrame) -> None:
    path = _cache_path(exchange)
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, path)
