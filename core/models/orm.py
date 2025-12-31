"""
SQLAlchemy ORM 模型定义

定义所有数据库表的ORM模型，用于数据加载层。
"""
from sqlalchemy import Boolean, Column, Index, PrimaryKeyConstraint, DECIMAL, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, DATE

from utils.date_helper import DateHelper

Base = declarative_base()


class DailyKlineORM(Base):
    """日线行情数据表ORM模型（存储未复权原始股价和前复权价格）"""
    __tablename__ = 'daily_kline'
    
    ts_code = Column(VARCHAR(12), nullable=False, primary_key=True, comment='股票代码')
    trade_date = Column(DATE, nullable=False, primary_key=True, comment='交易日期')
    open = Column(DECIMAL(10, 2), nullable=True, comment='未复权开盘价（元，精确到分）')
    high = Column(DECIMAL(10, 2), nullable=True, comment='未复权最高价（元，精确到分）')
    low = Column(DECIMAL(10, 2), nullable=True, comment='未复权最低价（元，精确到分）')
    close = Column(DECIMAL(10, 2), nullable=True, comment='未复权收盘价（元，精确到分）')
    change = Column(DECIMAL(10, 2), nullable=True, comment='涨跌额（元，精确到分）')
    vol = Column(BigInteger, nullable=True, comment='成交量（手）')
    amount = Column(DECIMAL(15, 2), nullable=True, comment='成交额（千元，精确到分）')
    # 前复权价格字段（可选，可能没有数据）
    close_qfq = Column(DECIMAL(10, 2), nullable=True, comment='前复权收盘价（元，精确到分）')
    open_qfq = Column(DECIMAL(10, 2), nullable=True, comment='前复权开盘价（元，精确到分）')
    high_qfq = Column(DECIMAL(10, 2), nullable=True, comment='前复权最高价（元，精确到分）')
    low_qfq = Column(DECIMAL(10, 2), nullable=True, comment='前复权最低价（元，精确到分）')
    
    __table_args__ = (
        PrimaryKeyConstraint('ts_code', 'trade_date'),
        Index('idx_ts_code', 'ts_code'),
        Index('idx_trade_date', 'trade_date'),
        {'comment': '日线行情数据表（未复权原始数据 + 前复权价格）'}
    )
    
    @staticmethod
    def _model_to_dict(model_instance) -> dict:
        """将ORM模型实例转换为字典"""
        result = {}
        for column in model_instance.__table__.columns:
            value = getattr(model_instance, column.name)
            # 处理日期类型：使用DateHelper统一转换为YYYYMMDD格式
            if value is not None and hasattr(value, 'strftime'):
                value = DateHelper.parse_to_str(value)
            elif isinstance(value, DECIMAL):
                value = float(value)
            result[column.name] = value
        return result


class BasicInfoORM(Base):
    """股票基本信息表ORM模型"""
    __tablename__ = 'basic_info'
    
    ts_code = Column(VARCHAR(12), primary_key=True, nullable=False, comment='股票代码')
    symbol = Column(VARCHAR(10), nullable=True, comment='股票简称代码')
    name = Column(VARCHAR(50), nullable=True, comment='股票名称')
    area = Column(VARCHAR(20), nullable=True, comment='地域')
    industry = Column(VARCHAR(50), nullable=True, comment='行业')
    market = Column(VARCHAR(20), nullable=True, comment='市场类型')
    list_date = Column(DATE, nullable=True, comment='上市日期')
    list_status = Column(VARCHAR(1), nullable=True, comment='上市状态')
    is_hs = Column(VARCHAR(1), nullable=True, comment='是否沪深港通标的')
    exchange = Column(VARCHAR(10), nullable=True, comment='交易所')
    updated_at = Column(DATE, nullable=True, comment='更新时间')
    
    __table_args__ = (
        Index('idx_basic_info_symbol', 'symbol'),
        Index('idx_basic_info_industry', 'industry'),
        {'comment': '股票基本信息表'}
    )


class TradeCalendarORM(Base):
    """交易日历表ORM模型"""
    __tablename__ = 'trade_calendar'
    
    cal_date = Column(DATE, nullable=False, primary_key=True, comment='日历日期')
    sse_open = Column(Boolean, nullable=True, comment='SSE是否交易（0=休市 1=交易）')
    szse_open = Column(Boolean, nullable=True, comment='SZSE是否交易（0=休市 1=交易）')
    cffex_open = Column(Boolean, nullable=True, comment='CFFEX是否交易（0=休市 1=交易）')
    shfe_open = Column(Boolean, nullable=True, comment='SHFE是否交易（0=休市 1=交易）')
    czce_open = Column(Boolean, nullable=True, comment='CZCE是否交易（0=休市 1=交易）')
    dce_open = Column(Boolean, nullable=True, comment='DCE是否交易（0=休市 1=交易）')
    ine_open = Column(Boolean, nullable=True, comment='INE是否交易（0=休市 1=交易）')
    
    __table_args__ = (
        PrimaryKeyConstraint('cal_date'),
        Index('idx_calendar_date', 'cal_date'),
        {'comment': '交易日历表'}
    )


class AdjFactorORM(Base):
    """复权因子表ORM模型（仅存储除权日的复权因子）"""
    __tablename__ = 'adj_factor'
    
    ts_code = Column(VARCHAR(12), nullable=False, primary_key=True, comment='股票代码')
    trade_date = Column(DATE, nullable=False, primary_key=True, comment='复权因子生效日期（除权除息日）')
    adj_factor = Column(DECIMAL(10, 4), nullable=False, comment='后复权因子（保留4位小数保证计算精度）')

    __table_args__ = (
        PrimaryKeyConstraint('ts_code', 'trade_date'),
        Index('idx_adj_factor_ts_code', 'ts_code'),
        Index('idx_adj_factor_date', 'trade_date'),
        {'comment': '复权因子表（仅存储除权除息日的复权因子）'}
    )

