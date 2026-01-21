from typing import List, Optional

from sqlalchemy import text

from backend.src.models.daily_kline import DailyKlineItem
from backend.src.services.database import get_session_factory


def _normalize_date(value: Optional[str]) -> Optional[str]:
    """标准化日期格式为 YYYY-MM-DD"""
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def fetch_daily_klines(
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[DailyKlineItem]:
    """
    获取股票日线行情数据
    
    性能优化：
    - 使用主键索引 (ts_code, trade_date) 进行高效查询
    - 日期范围查询利用 idx_trade_date 索引
    - 按 trade_date ASC 排序，利用索引顺序
    
    Args:
        ts_code: 股票代码
        start_date: 起始日期（YYYYMMDD 或 YYYY-MM-DD）
        end_date: 结束日期（YYYYMMDD 或 YYYY-MM-DD）
        limit: 最大返回记录数（可选，用于限制大数据量查询）
    
    Returns:
        日线行情数据列表，按交易日期升序排列
    """
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)

    # 构建查询，利用主键索引 (ts_code, trade_date)
    # 查询顺序：先按 ts_code 过滤（使用 idx_ts_code），再按 trade_date 范围过滤
    query = """
        SELECT trade_date, `open`, high, low, close, vol, amount, adj_factor
        FROM daily_kline
        WHERE ts_code = :ts_code
    """
    params = {"ts_code": ts_code}

    if start_date:
        query += " AND trade_date >= :start_date"
        params["start_date"] = start_date
    if end_date:
        query += " AND trade_date <= :end_date"
        params["end_date"] = end_date

    # 按交易日期升序排列，利用索引顺序
    query += " ORDER BY trade_date ASC"
    
    # 如果指定了限制，添加 LIMIT 子句（用于防止返回过多数据）
    if limit is not None and limit > 0:
        query += " LIMIT :limit"
        params["limit"] = limit

    session_factory = get_session_factory()
    with session_factory() as session:
        rows = session.execute(text(query), params).fetchall()

    # 转换为模型对象
    items: List[DailyKlineItem] = []
    for row in rows:
        items.append(
            DailyKlineItem(
                trade_date=str(row.trade_date),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.vol),
                amount=float(row.amount) if row.amount is not None else None,
                adj_factor=float(row.adj_factor)
                if row.adj_factor is not None
                else None,
            )
        )

    return items
