from typing import List

from sqlalchemy import text

from backend.src.models.data_update_status import DataUpdateStatusItem
from backend.src.services.database import get_session_factory


def fetch_data_update_status() -> List[DataUpdateStatusItem]:
    """
    获取数据更新状态
    
    查询日线数据表的最新更新时间和数据覆盖范围。
    利用索引 idx_trade_date 进行 MIN/MAX 聚合查询。
    
    Returns:
        数据更新状态列表，包含数据集名称、更新时间、覆盖范围等信息
    """
    session_factory = get_session_factory()
    with session_factory() as session:
        # 使用聚合函数查询数据范围，利用 trade_date 索引
        result = session.execute(
            text(
                """
                SELECT MIN(trade_date) AS min_date,
                       MAX(trade_date) AS max_date
                FROM daily_kline
                """
            )
        ).mappings().first()

    if not result or not result.get("max_date"):
        return [
            DataUpdateStatusItem(
                dataset="daily_kline",
                last_updated_at="",
                coverage_start=None,
                coverage_end=None,
                status="warning",
                message="暂无日线数据",
            )
        ]

    return [
        DataUpdateStatusItem(
            dataset="daily_kline",
            last_updated_at=str(result["max_date"]),
            coverage_start=str(result["min_date"]),
            coverage_end=str(result["max_date"]),
            status="ok",
            message="日线数据可用",
        )
    ]
