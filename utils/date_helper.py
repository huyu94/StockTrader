from datetime import date, datetime, timedelta
from typing import Union

class DateHelper:
    """
    日期处理辅助类
    
    统一管理日期格式转换，确保整个项目中日期格式的一致性：
    - 外部输入（scripts层）：接受 YYYYMMDD 或 YYYY-MM-DD，统一转换为 YYYY-MM-DD
    - 内部使用（fetch模块及以下）：统一使用 YYYY-MM-DD（MySQL DATE格式）
    - 数据库存储：YYYY-MM-DD（MySQL DATE格式）
    - API调用：需要时转换为 YYYYMMDD（如 Tushare API）
    """
    
    @staticmethod
    def normalize_to_yyyy_mm_dd(date_str: str) -> str:
        """
        标准化日期格式为 YYYY-MM-DD（MySQL DATE格式）
        
        支持输入格式：
        - YYYYMMDD（8位数字）
        - YYYY-MM-DD（10位字符串）
        
        :param date_str: 输入日期字符串
        :return: YYYY-MM-DD 格式的日期字符串
        :raises ValueError: 如果日期格式无效
        """
        if not date_str:
            raise ValueError("Date string cannot be empty")
        
        date_str = str(date_str).strip()
        
        # YYYY-MM-DD -> 验证后返回
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                return date_str
            except ValueError:
                raise ValueError(f"Invalid date format: {date_str}")
        
        # YYYYMMDD -> YYYY-MM-DD
        elif len(date_str) == 8 and date_str.isdigit():
            try:
                datetime.strptime(date_str, '%Y%m%d')
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except ValueError:
                raise ValueError(f"Invalid date format: {date_str}")
        
        else:
            raise ValueError(f"Unsupported date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")

    @staticmethod
    def normalize_to_yyyymmdd(date_str: str) -> str:
        """
        标准化日期格式为 YYYYMMDD（项目统一格式）
        
        支持输入格式：
        - YYYYMMDD（8位数字）
        - YYYY-MM-DD（10位字符串）
        
        :param date_str: 输入日期字符串
        :return: YYYYMMDD 格式的日期字符串
        :raises ValueError: 如果日期格式无效
        """
        if not date_str:
            raise ValueError("Date string cannot be empty")
        
        date_str = str(date_str).strip()
        
        # YYYYMMDD -> 验证后返回
        if len(date_str) == 8 and date_str.isdigit():
            try:
                datetime.strptime(date_str, '%Y%m%d')
                return date_str
            except ValueError:
                raise ValueError(f"Invalid date format: {date_str}")
        
        # YYYY-MM-DD -> YYYYMMDD
        elif len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
                return date_str.replace('-', '')
            except ValueError:
                raise ValueError(f"Invalid date format: {date_str}")
        
        else:
            raise ValueError(f"Unsupported date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
    
    @staticmethod
    def to_display(date_str: str) -> str:
        """
        将 YYYYMMDD 格式转换为 YYYY-MM-DD（用于显示）
        
        :param date_str: YYYYMMDD 格式的日期字符串
        :return: YYYY-MM-DD 格式的日期字符串
        """
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str
    
    @staticmethod
    def today() -> str:
        """
        获取今天的日期（YYYY-MM-DD 格式）
        
        :return: 今天的日期字符串
        """
        return datetime.now().strftime('%Y-%m-%d')
    
    @staticmethod
    def days_ago(days: int) -> str:
        """
        获取N天前的日期（YYYY-MM-DD 格式）
        
        :param days: 天数
        :return: N天前的日期字符串
        """
        return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    @staticmethod
    def parse_to_str(date_obj: Union[date, datetime, str]) -> str:
        """
        将日期对象转换为YYYY-MM-DD格式字符串
        
        :param date_obj: 日期对象（date, datetime, str）
        :return: YYYY-MM-DD格式字符串
        """
        if isinstance(date_obj, datetime):
            return date_obj.strftime('%Y-%m-%d')
        elif isinstance(date_obj, date):
            return date_obj.strftime('%Y-%m-%d')
        elif isinstance(date_obj, str):
            return DateHelper.normalize_to_yyyy_mm_dd(date_obj)

    @staticmethod
    def parse_to_date(date_obj: Union[date, datetime, str]) -> date:
        """
        将日期字符串转换为date对象
        
        :param date_obj: 日期对象或字符串（支持 YYYY-MM-DD 或 YYYYMMDD 格式）
        :return: date对象
        """
        if isinstance(date_obj, datetime):
            return date_obj.date()
        elif isinstance(date_obj, date):
            return date_obj
        elif isinstance(date_obj, str):
            # 支持 YYYY-MM-DD 和 YYYYMMDD 两种格式
            date_str_normalized = DateHelper.normalize_to_yyyy_mm_dd(date_obj)
            return datetime.strptime(date_str_normalized, '%Y-%m-%d').date()
    
    @staticmethod
    def parse_to_datetime(date_obj: Union[date, datetime, str]) -> datetime:
        """
        将日期字符串转换为datetime对象
        
        :param date_obj: 日期对象或字符串（支持 YYYY-MM-DD 或 YYYYMMDD 格式）
        :return: datetime对象
        """
        if isinstance(date_obj, datetime):
            return date_obj
        elif isinstance(date_obj, date):
            return datetime.combine(date_obj, datetime.min.time())
        elif isinstance(date_obj, str):
            # 支持 YYYY-MM-DD 和 YYYYMMDD 两种格式
            date_str_normalized = DateHelper.normalize_to_yyyy_mm_dd(date_obj)
            return datetime.strptime(date_str_normalized, '%Y-%m-%d')
    
    