from typing import List, Optional, Tuple

from sqlalchemy import text

from backend.src.models.stock import StockItem
from backend.src.services.database import get_session_factory


def search_stocks(
    query: Optional[str],
    limit: int = 20,
    offset: int = 0,
) -> Tuple[List[StockItem], int]:
    """
    搜索股票列表
    
    支持按股票代码或名称进行模糊搜索，返回分页结果。
    
    Args:
        query: 搜索关键字（股票代码或名称），None 时返回所有股票
        limit: 每页返回数量（1-200）
        offset: 分页偏移量
    
    Returns:
        (股票列表, 总记录数) 元组
    """
    session_factory = get_session_factory()
    keyword = f"%{query}%" if query else "%"

    list_sql = """
        SELECT ts_code, name, market
        FROM stock_basic_info
        WHERE ts_code LIKE :keyword OR name LIKE :keyword
        ORDER BY ts_code ASC
        LIMIT :limit OFFSET :offset
    """

    count_sql = """
        SELECT COUNT(*) AS total
        FROM stock_basic_info
        WHERE ts_code LIKE :keyword OR name LIKE :keyword
    """

    params = {"keyword": keyword, "limit": limit, "offset": offset}
    with session_factory() as session:
        rows = session.execute(text(list_sql), params).fetchall()
        total = session.execute(text(count_sql), {"keyword": keyword}).scalar() or 0

    items = [
        StockItem(ts_code=row.ts_code, name=row.name, market=row.market) for row in rows
    ]
    return items, int(total)
