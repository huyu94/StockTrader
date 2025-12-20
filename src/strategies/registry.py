from typing import Any
from src.strategies.daily_recommend_strategy import ShoufuStrategy

_REGISTRY = {
    "少妇战法": ShoufuStrategy,
}

def get_strategy(name: str) -> Any:
    cls = _REGISTRY.get(name)
    if not cls:
        raise ValueError(f"未找到策略: {name}")
    return cls()

def available_strategies() -> list:
    return list(_REGISTRY.keys())
