"""
少妇战法策略 (KDJ Strategy)

策略名称：少妇战法
策略逻辑：
1. KDJ指标的J值必须小于等于5（超卖信号）
2. 当前成交量必须小于前20日成交量最大值的1/2（缩量信号）
3. 根据价格运行位置，检查是否接近对应均线且未跌破：
   - 如果运行在20日线上方，检查是否接近20日线且未跌破
   - 如果运行在30日线上方，检查是否接近30日线且未跌破
   - 如果运行在60日线上方，检查是否接近60日线且未跌破

买入条件：
- 同时满足以上三个条件时，认为符合买入条件
- 这是一个寻找超卖+缩量+价格回归均线且未跌破的策略，适合捕捉反弹机会
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
from .base_strategy import BaseStrategy
from loguru import logger


class KDJStrategy(BaseStrategy):
    """
    少妇战法策略
    
    策略说明：
    1. KDJ指标的J值 <= 5：表示股票处于超卖状态，可能反弹
    2. 成交量缩量：当前成交量 < 前20日最大成交量的1/2，表示抛压减轻
    3. 价格回归均线且未跌破：
       - 如果运行在MA20上方，检查是否接近MA20且未跌破
       - 如果运行在MA30上方，检查是否接近MA30且未跌破
       - 如果运行在MA60上方，检查是否接近MA60且未跌破
       - 优先级：MA20 > MA30 > MA60
    
    适用场景：
    - 适合捕捉超跌反弹机会
    - 适合在震荡市中使用
    - 需要结合其他指标确认买入时机
    - 根据价格运行位置动态调整均线检查，更灵活
    """
    
    def __init__(
        self, 
        storage=None, 
        kdj_period: int = 9, 
        vol_period: int = 20, 
        j_threshold: float = 5.0,
        ma_tolerance: float = 0.03
    ):
        """
        初始化策略
        
        :param storage: 日线数据存储对象（DailyKlineStorageSQLite实例）
        :param kdj_period: KDJ指标计算周期，默认9
        :param vol_period: 成交量比较周期，默认20
        :param j_threshold: J值阈值，默认5.0（J值必须小于等于此值）
        :param ma_tolerance: 接近均线的容忍度，默认0.03（3%），表示价格在均线的±3%范围内算接近
        """
        super().__init__(storage=storage, name="少妇战法")
        self.kdj_period = kdj_period
        self.vol_period = vol_period
        self.j_threshold = j_threshold
        self.ma_tolerance = ma_tolerance
    
    def _preprocess(self, ts_code: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算KDJ指标、成交量相关指标和均线相关指标
        
        使用基类提供的计算方法：
        1. calculate_kdj() - 计算KDJ指标（已在基类preprocess中计算）
        2. 计算成交量相关指标
        3. 计算均线相关指标
        
        :param ts_code: 股票代码
        :param df: 日线数据DataFrame（已包含KDJ等基础指标）
        :return: 包含KDJ、成交量和均线指标的DataFrame
        """
        df_copy = df.copy()
        
        # 确保数据按日期排序
        df_copy = df_copy.sort_values('trade_date').reset_index(drop=True)
        
        # ========== 计算成交量指标 ==========
        # 计算前N日的成交量最大值
        df_copy['vol_max_20'] = df_copy['vol'].rolling(window=self.vol_period).max()
        # 计算当前成交量与最大值的比例
        df_copy['vol_ratio'] = np.where(
            df_copy['vol_max_20'] > 0,
            df_copy['vol'] / df_copy['vol_max_20'],
            1.0  # 如果最大成交量为0，比例设为1
        )
        
        # print(df_copy[['trade_date', 'vol', 'vol_max_20', 'vol_ratio']])
        # 标记是否满足成交量缩量条件（当前成交量 < 最大成交量的1/2）
        df_copy['vol_shrink'] = df_copy['vol_ratio'] < 0.5
        
        # 标记是否满足J值条件（J值 <= 阈值）
        df_copy['kdj_oversold'] = df_copy['kdj_j'] <= self.j_threshold
        
        # ========== 计算均线指标 ==========
        # 计算MA20、MA30和MA60（如果还没有计算的话）
        if 'ma20' not in df_copy.columns:
            df_copy['ma20'] = df_copy['close'].rolling(window=20).mean()
        if 'ma30' not in df_copy.columns:
            df_copy['ma30'] = df_copy['close'].rolling(window=30).mean()
        if 'ma60' not in df_copy.columns:
            df_copy['ma60'] = df_copy['close'].rolling(window=60).mean()
        
        # 判断收盘价在哪个均线上方（优先级：MA20 > MA30 > MA60）
        df_copy['above_ma20'] = df_copy['close'] > df_copy['ma20']
        df_copy['above_ma30'] = df_copy['close'] > df_copy['ma30']
        df_copy['above_ma60'] = df_copy['close'] > df_copy['ma60']
        
        # 计算收盘价与各均线的偏离度
        df_copy['close_to_ma20'] = np.where(
            df_copy['ma20'] > 0,
            (df_copy['close'] - df_copy['ma20']) / df_copy['ma20'],
            np.nan
        )
        df_copy['close_to_ma30'] = np.where(
            df_copy['ma30'] > 0,
            (df_copy['close'] - df_copy['ma30']) / df_copy['ma30'],
            np.nan
        )
        df_copy['close_to_ma60'] = np.where(
            df_copy['ma60'] > 0,
            (df_copy['close'] - df_copy['ma60']) / df_copy['ma60'],
            np.nan
        )
        
        # 判断是否接近各均线（在容忍度范围内，且在上方）
        # 接近MA20：在MA20上方 且 偏离度 <= tolerance
        df_copy['near_ma20'] = (
            df_copy['above_ma20'] & 
            (df_copy['close_to_ma20'] <= self.ma_tolerance) &
            (df_copy['close_to_ma20'] >= 0)  # 确保在上方，不能跌破
        )
        
        # 接近MA30：在MA30上方 且 偏离度 <= tolerance（且不在MA20上方，因为优先级更高）
        df_copy['near_ma30'] = (
            df_copy['above_ma30'] & 
            ~df_copy['above_ma20'] &  # 不在MA20上方（因为如果在上方，应该检查MA20）
            (df_copy['close_to_ma30'] <= self.ma_tolerance) &
            (df_copy['close_to_ma30'] >= 0)  # 确保在上方，不能跌破
        )
        
        # 接近MA60：在MA60上方 且 偏离度 <= tolerance（且不在MA20和MA30上方）
        df_copy['near_ma60'] = (
            df_copy['above_ma60'] & 
            ~df_copy['above_ma20'] &  # 不在MA20上方
            ~df_copy['above_ma30'] &  # 不在MA30上方
            (df_copy['close_to_ma60'] <= self.ma_tolerance) &
            (df_copy['close_to_ma60'] >= 0)  # 确保在上方，不能跌破
        )
        
        # 判断是否满足条件：接近任一均线且未跌破
        df_copy['near_ma'] = df_copy['near_ma20'] | df_copy['near_ma30'] | df_copy['near_ma60']
        
        # 记录当前价格运行在哪个均线上方（用于日志）
        df_copy['running_ma'] = np.where(
            df_copy['above_ma20'], 'MA20',
            np.where(
                df_copy['above_ma30'], 'MA30',
                np.where(df_copy['above_ma60'], 'MA60', 'None')
            )
        )
        
        logger.info(df_copy)
        return df_copy



    
    def _check_stock(self, ts_code: str, df: pd.DataFrame, target_date: str = None) -> bool:
        """
        判断是否符合买入条件
        
        买入条件：
        1. KDJ的J值 <= 5（超卖信号）
        2. 当前成交量 < 前20日最大成交量的1/2（缩量信号）
        3. 收盘价接近MA20或MA30（价格回归均线）
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :param target_date: 目标检查日期（YYYYMMDD格式），如果为None则检查最新交易日
        :return: True表示符合买入条件
        """
        # 需要至少vol_period个交易日的数据
        if len(df) < self.vol_period:
            logger.debug(f"{ts_code}: 数据不足，需要至少{self.vol_period}个交易日")
            return False
        
        # 需要至少kdj_period个交易日的数据来计算KDJ
        if len(df) < self.kdj_period:
            logger.debug(f"{ts_code}: 数据不足，无法计算KDJ指标")
            return False
        
        # 需要至少30个交易日的数据来计算MA30
        if len(df) < 30:
            logger.debug(f"{ts_code}: 数据不足，需要至少30个交易日来计算MA30")
            return False
        
        # 需要至少60个交易日的数据来计算MA60
        if len(df) < 60:
            logger.debug(f"{ts_code}: 数据不足，需要至少60个交易日来计算MA60")
            return False
        
        # 根据 target_date 筛选目标数据
        if target_date is not None:
            # 确保 trade_date 是 datetime 类型
            if not pd.api.types.is_datetime64_any_dtype(df['trade_date']):
                df = df.copy()
                df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
            
            # 转换为字符串格式进行比较
            df['trade_date_str'] = df['trade_date'].dt.strftime('%Y%m%d')
            
            # 筛选目标日期的数据
            target_data = df[df['trade_date_str'] == target_date]
            # logger.info(f"{ts_code}: 找到 {target_date} 的数据，数据量: {target_data[['trade_date', 'vol_ratio']]}")
            if target_data.empty:
                logger.warning(f"{ts_code}: 未找到 {target_date} 的数据，可能不是交易日")
                return False
            
            target_row = target_data.iloc[0]
        else:
            # 如果没有指定目标日期，使用最新的交易日
            target_row = df.iloc[-1]
        
        # 检查J值是否有效
        if pd.isna(target_row.get('kdj_j')):
            logger.debug(f"{ts_code}: KDJ的J值无效")
            return False
        
        # 检查成交量数据是否有效
        if pd.isna(target_row.get('vol')) or pd.isna(target_row.get('vol_max_20')):
            logger.debug(f"{ts_code}: 成交量数据无效")
            return False
        
        # 检查均线数据是否有效
        if pd.isna(target_row.get('ma20')) or pd.isna(target_row.get('ma30')) or pd.isna(target_row.get('ma60')):
            logger.debug(f"{ts_code}: 均线数据无效")
            return False
        
        # 条件1：KDJ的J值 <= 阈值（超卖信号）
        j_value = target_row.get('kdj_j', 999)
        condition1 = j_value <= self.j_threshold
        
        # 条件2：当前成交量 < 前20日最大成交量的1/2（缩量信号）
        # logger.info(f"{ts_code}: 当前成交量: {target_row.get('vol', 0)}, 前20日最大成交量: {target_row.get('vol_max_20', 0)}, 成交量比例: {target_row.get('vol_ratio', 1.0)}")
        vol_ratio = target_row.get('vol_ratio', 1.0)
        condition2 = vol_ratio < 0.5
        
        # 条件3：根据价格运行位置，检查是否接近对应均线且未跌破
        # 如果运行在MA20上方，检查是否接近MA20且未跌破
        # 如果运行在MA30上方，检查是否接近MA30且未跌破
        # 如果运行在MA60上方，检查是否接近MA60且未跌破
        near_ma = target_row.get('near_ma', False)
        condition3 = bool(near_ma) if not pd.isna(near_ma) else False
        
        # 三个条件都必须满足
        result = condition1 and condition2 and condition3
        
        # 获取交易日期字符串
        check_date_str = target_date if target_date else "最新交易日"
        
        # 获取均线信息用于日志
        close_price = target_row.get('close', 0)
        ma20 = target_row.get('ma20', 0)
        ma30 = target_row.get('ma30', 0)
        ma60 = target_row.get('ma60', 0)
        above_ma20 = target_row.get('above_ma20', False)
        above_ma30 = target_row.get('above_ma30', False)
        above_ma60 = target_row.get('above_ma60', False)
        near_ma20 = target_row.get('near_ma20', False)
        near_ma30 = target_row.get('near_ma30', False)
        near_ma60 = target_row.get('near_ma60', False)
        running_ma = target_row.get('running_ma', 'None')
        
        # 添加详细的调试信息
        if result:
            ma_info = []
            if near_ma20:
                ma_info.append(f"运行在MA20上方，接近MA20({ma20:.2f})且未跌破")
            elif near_ma30:
                ma_info.append(f"运行在MA30上方，接近MA30({ma30:.2f})且未跌破")
            elif near_ma60:
                ma_info.append(f"运行在MA60上方，接近MA60({ma60:.2f})且未跌破")
            
            logger.debug(
                f"{ts_code} ({check_date_str}): ✓ 符合少妇战法条件 - "
                f"J值={j_value:.2f} <= {self.j_threshold}, "
                f"成交量比例={vol_ratio:.2%} < 50%, "
                f"收盘价={close_price:.2f}, " + ", ".join(ma_info)
            )
        else:
            # 输出不满足条件的原因
            reasons = []
            if not condition1:
                reasons.append(f"J值={j_value:.2f} > {self.j_threshold}")
            if not condition2:
                reasons.append(f"成交量比例={vol_ratio:.2%} >= 50%")
            if not condition3:
                # 详细说明为什么不满足条件3
                if above_ma20:
                    if not near_ma20:
                        reasons.append(
                            f"运行在MA20上方，但收盘价={close_price:.2f}不接近MA20({ma20:.2f})或已跌破"
                        )
                elif above_ma30:
                    if not near_ma30:
                        reasons.append(
                            f"运行在MA30上方，但收盘价={close_price:.2f}不接近MA30({ma30:.2f})或已跌破"
                        )
                elif above_ma60:
                    if not near_ma60:
                        reasons.append(
                            f"运行在MA60上方，但收盘价={close_price:.2f}不接近MA60({ma60:.2f})或已跌破"
                        )
                else:
                    reasons.append(
                        f"收盘价={close_price:.2f}未在任何均线上方（MA20={ma20:.2f}, MA30={ma30:.2f}, MA60={ma60:.2f}）"
                    )
            logger.debug(
                f"{ts_code} ({check_date_str}): ✗ 不符合条件 - " + ", ".join(reasons)
            )
        
        return result
    
    #TODO: 这里还是最新日期的，有问题
    def _explain(self, ts_code: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        生成策略解释信息
        
        :param ts_code: 股票代码
        :param df: 已预处理的数据DataFrame
        :return: 解释信息字典
        """
        latest = df.iloc[-1]
        
        j_value = float(latest.get('kdj_j', 0))
        vol_ratio = float(latest.get('vol_ratio', 1.0))
        close_price = float(latest.get('close', 0))
        ma20 = float(latest.get('ma20', 0))
        ma30 = float(latest.get('ma30', 0))
        ma60 = float(latest.get('ma60', 0))
        near_ma20 = bool(latest.get('near_ma20', False))
        near_ma30 = bool(latest.get('near_ma30', False))
        near_ma60 = bool(latest.get('near_ma60', False))
        running_ma = str(latest.get('running_ma', 'None'))
        
        # 构建均线信息
        ma_info = ""
        if near_ma20:
            ma_info = f"运行在MA20上方，接近MA20({ma20:.2f})且未跌破"
        elif near_ma30:
            ma_info = f"运行在MA30上方，接近MA30({ma30:.2f})且未跌破"
        elif near_ma60:
            ma_info = f"运行在MA60上方，接近MA60({ma60:.2f})且未跌破"
        else:
            ma_info = f"不满足均线条件（当前运行在{running_ma}上方）"
        
        result = {
            "reason": (
                f"KDJ的J值={j_value:.2f} <= {self.j_threshold}（超卖信号），"
                f"成交量={vol_ratio:.2%} < 50%（缩量信号），"
                f"收盘价={close_price:.2f}，{ma_info}（价格回归均线且未跌破）"
            )
        }
        
        return result
