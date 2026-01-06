"""
ETL Pipeline 核心模块

提供完整的 ETL Pipeline 架构，包括：
- Collectors: 数据采集层
- Transformers: 数据转换层
- Loaders: 数据加载层
- Pipelines: 流水线编排层
- Models: 数据模型
- Orchestrator: 调度编排层
- Config: 配置管理
- Common: 公共组件
"""

# 采集器
from core.collectors import (
    BaseCollector,
    DailyKlineCollector,
    AdjFactorCollector,
    ExDateCollector,
    BasicInfoCollector,
    TradeCalendarCollector,
)

# 转换器
from core.transformers import (
    BaseTransformer,
    DailyKlineTransformer,
    AdjFactorTransformer,
    BasicInfoTransformer,
    TradeCalendarTransformer,
)

# 加载器
from core.loaders import (
    BaseLoader,
    DailyKlineLoader,
    AdjFactorLoader,
    BasicInfoLoader,
    TradeCalendarLoader,
)

# 流水线
from core.pipelines import (
    BasePipeline,
    DailyPipeline,
    HistoryPipeline,
    RealtimePipeline,
)

# 数据模型
from core.models import (
    DailyKline,
    AdjFactor,
    StockBasicInfo,
    TradeCalendar,
)

# 调度编排
from core.orchestrator import (
    TaskScheduler,
    DependencyManager,
    TaskMonitor,
    TaskStatus,
)

# 公共组件
from core.common import (
    PipelineException,
    CollectorException,
    TransformerException,
    LoaderException,
    ConfigException,
    ValidationException,
    DataSourceException,
    NetworkException,
    DatabaseException,
    DataValidator,
)

__all__ = [
    # 采集器
    "BaseCollector",
    "DailyKlineCollector",
    "AdjFactorCollector",
    "ExDateCollector",
    "BasicInfoCollector",
    "TradeCalendarCollector",
    # 转换器
    "BaseTransformer",
    "DailyKlineTransformer",
    "AdjFactorTransformer",
    "BasicInfoTransformer",
    "TradeCalendarTransformer",
    # 加载器
    "BaseLoader",
    "DailyKlineLoader",
    "AdjFactorLoader",
    "BasicInfoLoader",
    "TradeCalendarLoader",
    # 流水线
    "BasePipeline",
    "DailyPipeline",
    "HistoryPipeline",
    "RealtimePipeline",
    # 数据模型
    "DailyKline",
    "AdjFactor",
    "StockBasicInfo",
    "TradeCalendar",
    # 调度编排
    "TaskScheduler",
    "DependencyManager",
    "TaskMonitor",
    "TaskStatus",
    # 异常
    "PipelineException",
    "CollectorException",
    "TransformerException",
    "LoaderException",
    "ConfigException",
    "ValidationException",
    "DataSourceException",
    "NetworkException",
    "DatabaseException",
    # 公共组件
    "DataValidator",
]

