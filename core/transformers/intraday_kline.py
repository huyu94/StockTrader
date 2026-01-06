"""
分时K线数据转换器

负责清洗、标准化分时K线数据
"""

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger

from core.transformers.base import BaseTransformer
from core.common.exceptions import TransformerException
from utils.date_helper import DateHelper


class IntradayKlineTransformer(BaseTransformer):
    """
    分时K线数据转换器
    
    对采集到的分时K线数据进行清洗、标准化处理：
    - 字段重命名和映射
    - 数据类型转换
    - 剔除异常数据
    - 日期时间格式标准化
    """
    
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        转换分时K线数据
        
        Args:
            data: 原始分时K线数据（可能是 akshare 返回的原始格式）
            **kwargs: 转换参数（可选）
                - ts_codes: List[str] 或 str, 股票代码列表（可选，用于过滤）
                - trade_date: str, 交易日期 (YYYY-MM-DD)（可选，如果不提供则使用当前日期）
            
        Returns:
            pd.DataFrame: 转换后的分时K线数据，包含标准化后的字段：
                - ts_code: 股票代码
                - trade_date: 交易日期 (YYYY-MM-DD)
                - time: 时间 (HH:MM:SS)
                - datetime: 完整时间戳 (YYYY-MM-DD HH:MM:SS)（可选）
                - price: 价格（元，精确到分）
                - volume: 成交量（手）
                - amount: 成交额（元，精确到分）
            
        Raises:
            TransformerException: 转换失败时抛出异常
        """
        if data is None or data.empty:
            logger.warning("输入数据为空，返回空 DataFrame")
            return pd.DataFrame()
        
        logger.debug(f"开始转换分时K线数据，数据量: {len(data)}")
        
        try:
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 构建 params 字典（兼容旧接口）
            params = {}
            if 'ts_codes' in kwargs:
                params['ts_codes'] = kwargs['ts_codes']
            if 'trade_date' in kwargs:
                params['trade_date'] = kwargs['trade_date']
            
            # 检查是否是 akshare 原始数据格式（包含中文列名）
            if '代码' in df.columns or '最新价' in df.columns:
                # 这是 akshare 返回的原始数据，需要标准化
                df = self._normalize_akshare_data(df, params)
            
            # 1. 字段重命名（如果需要）
            column_mapping = self.transform_rules.get("column_mapping", {})
            if column_mapping:
                df = self._rename_columns(df, column_mapping)
            
            # 2. 标准化日期格式
            if 'trade_date' in df.columns:
                df['trade_date'] = df['trade_date'].apply(
                    lambda x: DateHelper.normalize_to_yyyy_mm_dd(str(x)) if pd.notna(x) else None
                )
            
            # 3. 标准化时间格式
            if 'time' in df.columns:
                df['time'] = df['time'].apply(
                    lambda x: self._normalize_time(str(x)) if pd.notna(x) else None
                )
            
            # 4. 构建 datetime 列（如果不存在）
            if 'datetime' not in df.columns:
                if 'trade_date' in df.columns and 'time' in df.columns:
                    df['datetime'] = df['trade_date'].astype(str) + ' ' + df['time'].astype(str)
            
            # 5. 数据类型转换
            numeric_columns = ['price', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    if col == 'volume':
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 6. 剔除异常数据
            initial_count = len(df)
            
            # 剔除价格为0或负数的数据
            if 'price' in df.columns:
                df = df[df['price'] > 0]
            
            # 剔除成交量为负数的数据
            if 'volume' in df.columns:
                df = df[df['volume'] >= 0]
            
            # 剔除成交额为负数的数据
            if 'amount' in df.columns:
                df = df[df['amount'] >= 0]
            
            removed_count = initial_count - len(df)
            if removed_count > 0:
                logger.info(f"剔除异常数据: {removed_count} 条")
            
            # 7. 处理缺失值
            if self.transform_rules.get("fill_missing", False):
                # 对于价格数据，使用前一个时间点的价格填充
                if 'price' in df.columns:
                    df = df.sort_values(['ts_code', 'trade_date', 'time'])
                    df['price'] = df.groupby(['ts_code', 'trade_date'])['price'].ffill()
            
            # 8. 确保必需字段存在
            required_columns = ['ts_code', 'trade_date', 'time', 'price', 'volume', 'amount']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformerException(f"缺少必需的列: {missing_columns}")
            
            # 9. 确保输出列的顺序和完整性（符合数据库模型）
            output_columns = ['ts_code', 'trade_date', 'time', 'datetime', 'price', 'volume', 'amount']
            # 只选择存在的列
            existing_columns = [col for col in output_columns if col in df.columns]
            df = df[existing_columns].copy()
            
            # 10. 按股票代码、日期、时间排序
            if 'ts_code' in df.columns and 'trade_date' in df.columns and 'time' in df.columns:
                df = df.sort_values(['ts_code', 'trade_date', 'time']).reset_index(drop=True)
            
            # 11. 将 nan 值转换为 None，确保数据库兼容性
            df = df.where(pd.notna(df), None)
            
            logger.debug(f"转换完成，最终数据量: {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"转换分时K线数据失败: {e}")
            raise TransformerException(f"转换分时K线数据失败: {e}") from e
    
    def _normalize_akshare_data(
        self,
        df: pd.DataFrame,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        标准化 akshare 实时行情数据格式为分时数据格式
        
        Args:
            df: akshare 返回的原始数据
            params: 转换参数
                - ts_codes: List[str] 或 str, 股票代码列表（可选，用于过滤）
                - trade_date: str, 交易日期 (YYYY-MM-DD)（可选，如果不提供则使用当前日期）
            
        Returns:
            pd.DataFrame: 标准化后的分时数据
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        if params is None:
            params = {}
        
        df = df.copy()
        
        # akshare 返回的列名映射
        # 根据 akshare_api.md，列名包括：代码、最新价、成交量、成交额等
        column_mapping = {
            '代码': 'code',  # 股票代码
            '最新价': 'price',  # 最新价
            '成交量': 'volume',  # 成交量（手）
            '成交额': 'amount',  # 成交额（元）
        }
        
        # 重命名列
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # 如果没有找到标准列名，尝试其他可能的列名
        if 'code' not in df.columns:
            # 尝试其他可能的列名
            possible_code_cols = ['代码', 'code', 'ts_code', 'symbol', '股票代码']
            for col in possible_code_cols:
                if col in df.columns:
                    df['code'] = df[col]
                    break
        
        if 'price' not in df.columns:
            possible_price_cols = ['最新价', 'price', 'close', '现价', '当前价']
            for col in possible_price_cols:
                if col in df.columns:
                    df['price'] = df[col]
                    break
        
        if 'volume' not in df.columns:
            possible_volume_cols = ['成交量', 'volume', 'vol', '成交手数']
            for col in possible_volume_cols:
                if col in df.columns:
                    df['volume'] = df[col]
                    break
        
        if 'amount' not in df.columns:
            possible_amount_cols = ['成交额', 'amount', 'money', '成交金额']
            for col in possible_amount_cols:
                if col in df.columns:
                    df['amount'] = df[col]
                    break
        
        # 检查必需列
        required_columns = ['code', 'price', 'volume', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise TransformerException(f"akshare 返回的数据缺少必需的列: {missing_columns}，可用列: {df.columns.tolist()}")
        
        # 转换股票代码格式（akshare 返回的可能是 6 位数字，需要转换为 ts_code 格式）
        def convert_code_to_ts_code(code):
            """将股票代码转换为 ts_code 格式（如 000001 -> 000001.SZ）"""
            if pd.isna(code):
                return None
            
            code_str = str(code).strip()
            # 如果是 6 位数字，需要判断是深交所还是上交所
            if len(code_str) == 6 and code_str.isdigit():
                # 简单判断：60开头是上交所，00/30开头是深交所
                if code_str.startswith('60'):
                    return f"{code_str}.SH"
                elif code_str.startswith('00') or code_str.startswith('30'):
                    return f"{code_str}.SZ"
                elif code_str.startswith('68'):
                    return f"{code_str}.SH"  # 科创板
                elif code_str.startswith('43') or code_str.startswith('83'):
                    return f"{code_str}.BJ"  # 北交所
                else:
                    return f"{code_str}.SZ"  # 默认深交所
            # 如果已经是 ts_code 格式，直接返回
            elif '.' in code_str:
                return code_str
            else:
                return code_str
        
        df['ts_code'] = df['code'].apply(convert_code_to_ts_code)
        
        # 过滤股票代码（如果指定了）
        ts_codes_list = params.get('ts_codes')
        if ts_codes_list is not None:
            # 统一处理 ts_codes
            if isinstance(ts_codes_list, str):
                ts_codes_list = [ts_codes_list]
            elif isinstance(ts_codes_list, list):
                ts_codes_list = ts_codes_list if ts_codes_list else None
            else:
                ts_codes_list = None
            
            if ts_codes_list:
                # 标准化 ts_codes_list 中的代码格式
                normalized_codes = []
                for code in ts_codes_list:
                    if '.' in code:
                        normalized_codes.append(code)
                    else:
                        # 尝试转换
                        converted = convert_code_to_ts_code(code)
                        if converted:
                            normalized_codes.append(converted)
                
                df = df[df['ts_code'].isin(normalized_codes)]
        
        if df.empty:
            logger.warning("过滤后没有数据")
            return pd.DataFrame()
        
        # 获取交易日期
        trade_date = params.get('trade_date')
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        else:
            trade_date = DateHelper.normalize_to_yyyy_mm_dd(trade_date)
        
        # 添加日期和时间信息
        current_time = datetime.now()
        df['trade_date'] = trade_date
        df['time'] = current_time.strftime('%H:%M:%S')
        df['datetime'] = f"{trade_date} {current_time.strftime('%H:%M:%S')}"
        
        # 数据类型转换
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('Int64')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # 选择并重命名最终列
        result_df = df[['ts_code', 'trade_date', 'time', 'datetime', 'price', 'volume', 'amount']].copy()
        
        # 按股票代码排序
        result_df = result_df.sort_values('ts_code').reset_index(drop=True)
        
        return result_df
    
    def _normalize_time(self, time_str: str) -> str:
        """
        标准化时间格式为 HH:MM:SS
        
        Args:
            time_str: 时间字符串，可能是各种格式
            
        Returns:
            str: 标准化后的时间字符串 (HH:MM:SS)
        """
        if not time_str or time_str.strip() == '':
            return None
        
        # 移除空格
        time_str = time_str.strip()
        
        # 尝试解析各种格式
        try:
            # 如果已经是 HH:MM:SS 格式
            if len(time_str) == 8 and time_str.count(':') == 2:
                parts = time_str.split(':')
                if len(parts) == 3 and all(part.isdigit() for part in parts):
                    return time_str
            
            # 如果是 HH:MM 格式，补充秒
            if len(time_str) == 5 and time_str.count(':') == 1:
                return time_str + ':00'
            
            # 如果是 HHMMSS 格式（无冒号）
            if len(time_str) == 6 and time_str.isdigit():
                return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            
            # 如果是 HHMM 格式（无冒号）
            if len(time_str) == 4 and time_str.isdigit():
                return f"{time_str[:2]}:{time_str[2:4]}:00"
            
            # 尝试使用 pandas 解析
            from datetime import datetime
            parsed_time = pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')
            if pd.notna(parsed_time):
                return parsed_time.strftime('%H:%M:%S')
            
            logger.warning(f"无法解析时间格式: {time_str}，返回原值")
            return time_str
            
        except Exception as e:
            logger.warning(f"标准化时间格式失败: {time_str}, 错误: {e}")
            return time_str

