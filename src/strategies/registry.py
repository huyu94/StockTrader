from typing import Any
# from src.strategies.shaofu_strategy import ShaofuStrategy
from src.strategies.shaofu_strategy import ShaofuStrategy

_REGISTRY = {
    "少妇战法": ShaofuStrategy,
    # "少妇战法（简化）": ShaofuSimpleStrategy,
}

def get_strategy(name: str) -> Any:
    cls = _REGISTRY.get(name)
    if not cls:
        raise ValueError(f"未找到策略: {name}")
    return cls()

def available_strategies() -> list:
    return list(_REGISTRY.keys())
