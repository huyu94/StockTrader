"""
分时K线数据采集器

负责从数据源采集分时K线数据
使用 akshare 的实时行情接口获取数据
"""

from typing import Any, Dict, List, Optional
import pandas as pd
from datetime import datetime
from loguru import logger
from tqdm import tqdm

from core.collectors.base import BaseCollector
from core.common.exceptions import CollectorException
from utils.date_helper import DateHelper


class IntradayKlineCollector(BaseCollector):
    """
    分时K线数据采集器
    
    从数据源（Tushare、Akshare等）采集股票的分时K线数据
    支持按股票代码和日期采集
    """
    
    def collect(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        采集分时K线数据（使用 akshare 实时行情接口）
        
        Args:
                
        Returns:
            pd.DataFrame: 分时K线数据，包含以下列：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - time: 时间 (HH:MM:SS)
                - datetime: 完整时间戳 (YYYY-MM-DD HH:MM:SS)
                - price: 价格（最新价）
                - volume: 成交量
                - amount: 成交额
                
        Raises:
            CollectorException: 采集失败时抛出异常
        """
        # 获取参数（都是可选的）
        ts_codes = params.get("ts_codes")
        
        
        # 统一处理 ts_codes（可选）
        if ts_codes is None:
            ts_codes_list = None  # None 表示获取全市场数据
        elif isinstance(ts_codes, str):
            ts_codes_list = [ts_codes]
        elif isinstance(ts_codes, list):
            ts_codes_list = ts_codes if ts_codes else None
        else:
            raise CollectorException(f"ts_codes 必须是 str、List[str] 或 None，当前类型: {type(ts_codes)}")
        
        logger.info(f"开始采集实时行情数据（akshare）: ts_codes={ts_codes_list}, 日期={trade_date_normalized}")
        
        # 使用 akshare 获取实时行情数据
        try:
            df = self._retry_collect(
                self._fetch_akshare_spot_data
            )
            
            if df is None or df.empty:
                logger.warning("未采集到任何数据")
                return pd.DataFrame()
            
            # 标准化数据格式
            df = self._normalize_akshare_data(df, trade_date_normalized, ts_codes_list)
            
            logger.info(f"采集完成，共 {len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"采集实时行情数据失败: {e}")
            raise CollectorException(f"采集实时行情数据失败: {e}") from e
    
    def _fetch_akshare_spot_data(self) -> pd.DataFrame:
        """
        使用 akshare 获取实时行情数据
        
        Returns:
            pd.DataFrame: akshare 返回的原始数据
        """
        try:
            import akshare as ak
            
            # 调用 akshare 的实时行情接口
            df = ak.stock_zh_a_spot_em()
            
            if df is None or df.empty:
                logger.warning("akshare 返回空数据")
                return pd.DataFrame()
            
            logger.debug(f"akshare 返回 {len(df)} 条实时行情数据")
            return df
            
        except ImportError:
            raise CollectorException("akshare 库未安装，请使用 'pip install akshare' 安装")
        except Exception as e:
            logger.error(f"调用 akshare 接口失败: {e}")
            raise CollectorException(f"调用 akshare 接口失败: {e}") from e
    
    def _normalize_akshare_data(
        self,
        df: pd.DataFrame,
        trade_date: str,
        ts_codes_list: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        标准化 akshare 实时行情数据格式为分时数据格式
        
        Args:
            df: akshare 返回的原始数据
            trade_date: 交易日期 (YYYY-MM-DD)
            ts_codes_list: 股票代码列表（可选，用于过滤）
            
        Returns:
            pd.DataFrame: 标准化后的分时数据
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
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
            raise CollectorException(f"akshare 返回的数据缺少必需的列: {missing_columns}，可用列: {df.columns.tolist()}")
        
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
        if ts_codes_list is not None:
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

