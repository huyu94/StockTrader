"""
SQLite 数据库表结构定义

统一管理所有表的 SQL 创建语句，确保类型一致性和可维护性。

注意：
- SQLite 的 DATE 类型实际上存储为 TEXT（格式：YYYY-MM-DD）
- 但在查询时可以使用日期函数进行日期比较和计算
- 使用 DATE 类型可以提高代码可读性和类型检查
"""

# ==================== Daily Kline Table ====================
DAILY_KLINE_TABLE = """
CREATE TABLE IF NOT EXISTS daily_kline (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    pre_close REAL,
    change REAL,
    pct_chg REAL,
    vol REAL,
    amount REAL,
    adj_factor REAL,
    PRIMARY KEY (ts_code, trade_date)
)
"""

DAILY_KLINE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ts_code ON daily_kline(ts_code)",
    "CREATE INDEX IF NOT EXISTS idx_trade_date ON daily_kline(trade_date)",
]

# ==================== Adj Factor Table ====================
ADJ_FACTOR_TABLE = """
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor REAL,
    PRIMARY KEY (ts_code, trade_date)
)
"""

ADJ_FACTOR_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_adj_factor_ts_code ON adj_factor(ts_code)",
    "CREATE INDEX IF NOT EXISTS idx_adj_factor_trade_date ON adj_factor(trade_date)",
]

# ==================== Basic Info Table ====================
BASIC_INFO_TABLE = """
CREATE TABLE IF NOT EXISTS basic_info (
    ts_code TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    area TEXT,
    industry TEXT,
    market TEXT,
    list_date DATE,
    list_status TEXT,
    is_hs TEXT,
    exchange TEXT,
    updated_at DATE
)
"""

BASIC_INFO_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_basic_info_symbol ON basic_info(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_basic_info_industry ON basic_info(industry)",
]

# ==================== Trade Calendar Table ====================
TRADE_CALENDAR_TABLE = """
CREATE TABLE IF NOT EXISTS trade_calendar (
    exchange TEXT NOT NULL,
    cal_date DATE NOT NULL,
    is_open INTEGER NOT NULL,
    PRIMARY KEY (exchange, cal_date)
)
"""

TRADE_CALENDAR_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_calendar_exchange ON trade_calendar(exchange)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_date ON trade_calendar(cal_date)",
]

# ==================== Migration Scripts ====================
# 用于将现有 TEXT 类型的日期列迁移为 DATE 类型
# 注意：SQLite 不支持直接修改列类型，需要重建表

def get_migration_sql_for_date_column(table_name: str, column_name: str) -> list:
    """
    生成将 TEXT 类型日期列迁移为 DATE 类型的 SQL 语句
    
    注意：SQLite 不支持 ALTER COLUMN，需要：
    1. 创建新表（带 DATE 类型）
    2. 复制数据（转换格式）
    3. 删除旧表
    4. 重命名新表
    
    这个方法返回 SQL 语句列表，需要按顺序执行
    """
    return [
        f"""
        -- Step 1: 创建临时表（带 DATE 类型）
        CREATE TABLE {table_name}_new AS 
        SELECT 
            *,
            CASE 
                WHEN {column_name} GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' 
                THEN substr({column_name}, 1, 4) || '-' || substr({column_name}, 5, 2) || '-' || substr({column_name}, 7, 2)
                ELSE {column_name}
            END AS {column_name}_new
        FROM {table_name}
        """,
        f"""
        -- Step 2: 删除旧列，重命名新列（需要重建表结构）
        -- 注意：这需要手动处理，因为 SQLite 不支持直接修改列类型
        """
    ]

