import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy

class ShaofuStrategy(BaseStrategy):
    """
    少妇战法策略
    核心条件（逐条同时满足）：
    1) 勾到大负值：KDJ 的 J 值处于负值区间且当日相对前日出现向上勾头
    2) 当日涨幅：-2% 至 +1.8%
    3) 缩量至地量：当日成交量不超过近 20 日地量的 105%
    4) 振幅限制：主板 <4%，双创（300/688）<7%
    5) 收盘价在 MA5、MA10（白/黄线）之上
    6) MACD 无顶背离：最近一段时间若出现价创新高但 MACD 柱峰降低则剔除
    """
    def __init__(self):
        super().__init__()

    def _is_dual_innovation(self, ts_code: str) -> bool:
        code = ts_code.split('.')[0]
        return code.startswith('300') or code.startswith('688')

    def _has_bearish_divergence(self, df: pd.DataFrame, lookback: int = 60, min_points: int = 20) -> bool:
        """
        顶背离判定（简化版）：
        近 60 日内取最近两个局部高点，若第二个高点价格更高但 MACD 柱峰更低，视为顶背离
        """
        recent = df.tail(lookback).copy()
        if len(recent) < min_points:
            return False
        close = recent['收盘价']
        hist = recent['MACD_HIST']
        peaks_idx = []
        for i in range(2, len(close)-2):
            if close.iloc[i] > close.iloc[i-1] and close.iloc[i] > close.iloc[i+1]:
                peaks_idx.append(i)
        if len(peaks_idx) < 2:
            return False
        i1, i2 = peaks_idx[-2], peaks_idx[-1]
        c1, c2 = close.iloc[i1], close.iloc[i2]
        h1, h2 = hist.iloc[i1], hist.iloc[i2]
        if c2 > c1 and h2 < h1:
            return True
        return False

    def _hook_to_large_negative(self, df: pd.DataFrame, j_threshold: float = 0.0) -> bool:
        """
        勾到大负值：
        J 值处于负值区间（<=0），且本日 J 高于前一日，表示在负区间出现向上拐点
        """
        if len(df) < 2:
            return False
        j_prev = df['J'].iloc[-2]
        j_curr = df['J'].iloc[-1]
        return (j_curr <= j_threshold) and (j_curr > j_prev)

    def cond_pct_range(self, df: pd.DataFrame, pct_min: float = -2.0, pct_max: float = 1.8) -> bool:
        if '涨跌幅' not in df.columns:
            raise KeyError("缺少列：涨跌幅")
        last = df.iloc[-1]
        pct = last['涨跌幅']
        return pct_min <= pct <= pct_max

    def cond_vol_low(self, df: pd.DataFrame, vol_window: int = 20, vol_allowance: float = 1.05) -> bool:
        if '成交量' not in df.columns:
            raise KeyError("缺少列：成交量")
        last = df.iloc[-1]
        vol = last['成交量']
        vol_min = df['成交量'].tail(vol_window).min()
        if vol_min == 0:
            raise ValueError("成交量窗口内最小值为0，数据不完整")
        return vol <= vol_min * vol_allowance

    def cond_amp_limit(self, ts_code: str, df: pd.DataFrame, amp_limit_mainboard: float = 4.0, amp_limit_dual: float = 7.0) -> bool:
        amp = self.amplitude(df)
        if self._is_dual_innovation(ts_code):
            return amp < amp_limit_dual
        return amp < amp_limit_mainboard

    def cond_above_ma(self, df: pd.DataFrame, ma_short_col: str = 'MA5', ma_long_col: str = 'MA10', require_ma_short: bool = True, require_ma_long: bool = True) -> bool:
        if require_ma_short and ma_short_col not in df.columns:
            raise KeyError(f"缺少列：{ma_short_col}")
        if require_ma_long and ma_long_col not in df.columns:
            raise KeyError(f"缺少列：{ma_long_col}")
        last = df.iloc[-1]
        price = last['收盘价']
        ok_short = True if not require_ma_short else price >= last[ma_short_col]
        ok_long = True if not require_ma_long else price >= last[ma_long_col]
        return ok_short and ok_long

    def cond_no_macd_bearish_divergence(self, df: pd.DataFrame, lookback: int = 60, min_points: int = 20) -> bool:
        return not self._has_bearish_divergence(df, lookback, min_points)

    def _ensure_required_columns(self, df: pd.DataFrame) -> None:
        required = {'收盘价', '成交量', '涨跌幅', 'MA5', 'MA10', 'MACD_HIST', 'K', 'D', 'J'}
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise KeyError(f"缺少必要列：{','.join(missing)}")

    def check_stock(
        self,
        ts_code: str,
        raw_df: pd.DataFrame,
        pct_min: float = -2.0,
        pct_max: float = 1.8,
        vol_window: int = 20,
        vol_allowance: float = 1.05,
        amp_limit_mainboard: float = 4.0,
        amp_limit_dual: float = 7.0,
        ma_short_col: str = 'MA5',
        ma_long_col: str = 'MA10',
        require_ma_short: bool = True,
        require_ma_long: bool = True,
        macd_divergence_lookback: int = 60,
        macd_divergence_min_points: int = 20,
        enable_macd_bearish_divergence_filter: bool = True,
        enable_j_hook: bool = True,
        j_threshold: float = 0.0
    ) -> bool:
        """
        核心筛选函数：
        输入原始日线数据，统一预处理并依次按策略条件过滤，全部满足时返回 True
        """
        df = self.preprocess(raw_df)
        if len(df) == 0:
            return False
        self._ensure_required_columns(df)
        condition1 = self.cond_pct_range(df, pct_min, pct_max)
        condition2 = self.cond_vol_low(df, vol_window, vol_allowance)
        condition3 = self.cond_amp_limit(ts_code, df, amp_limit_mainboard, amp_limit_dual)
        condition4 = self.cond_above_ma(df, ma_short_col, ma_long_col, require_ma_short, require_ma_long)
        condition5 = True if not enable_macd_bearish_divergence_filter else self.cond_no_macd_bearish_divergence(df, macd_divergence_lookback, macd_divergence_min_points)
        condition6 = True if not enable_j_hook else self._hook_to_large_negative(df, j_threshold)
        return (
            condition1 and
            condition2 and
            condition3 and
            condition4 and
            condition5 and
            condition6
        )

    def explain(self, ts_code: str, raw_df: pd.DataFrame) -> dict:
        """
        输出简要说明，用于结果展示与审阅
        返回字段包含：
        - 交易日期、收盘价、涨跌幅、振幅
        - 是否在 MA5/MA10 上方
        - KDJ 的 J 值与 MACD 柱值
        """
        df = self.preprocess(raw_df)
        last = df.iloc[-1]
        amp = self.amplitude(df)
        if '涨跌幅' not in df.columns:
            raise KeyError("缺少列：涨跌幅")
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "close": round(last['收盘价'], 2),
            "pct_chg": round(last['涨跌幅'], 2),
            "amp": round(amp, 2),
            "above_ma5_ma10": (last['收盘价'] >= last['MA5']) and (last['收盘价'] >= last['MA10']),
            "kdj_j": round(last['J'], 2),
            "macd_hist": round(last['MACD_HIST'], 4)
        }

    def debug_check(
        self,
        ts_code: str,
        raw_df: pd.DataFrame,
        pct_min: float = -2.0,
        pct_max: float = 1.8,
        vol_window: int = 20,
        vol_allowance: float = 1.05,
        amp_limit_mainboard: float = 4.0,
        amp_limit_dual: float = 7.0,
        ma_short_col: str = 'MA5',
        ma_long_col: str = 'MA10',
        require_ma_short: bool = True,
        require_ma_long: bool = True,
        macd_divergence_lookback: int = 60,
        macd_divergence_min_points: int = 20,
        enable_macd_bearish_divergence_filter: bool = True,
        enable_j_hook: bool = True,
        j_threshold: float = 0.0
    ) -> dict:
        df = self.preprocess(raw_df)
        self._ensure_required_columns(df)
        last = df.iloc[-1]
        pct = float(last['涨跌幅'])
        vol_today = float(last['成交量'])
        vol_min = float(df['成交量'].tail(vol_window).min())
        amp_val = float(self.amplitude(df))
        ma5 = float(last['MA5'])
        ma10 = float(last['MA10'])
        price = float(last['收盘价'])
        j = float(last['J'])
        macd_hist = float(last['MACD_HIST'])
        c1 = self.cond_pct_range(df, pct_min, pct_max)
        c2 = self.cond_vol_low(df, vol_window, vol_allowance)
        c3 = self.cond_amp_limit(ts_code, df, amp_limit_mainboard, amp_limit_dual)
        c4 = self.cond_above_ma(df, ma_short_col, ma_long_col, require_ma_short, require_ma_long)
        c5 = True if not enable_macd_bearish_divergence_filter else self.cond_no_macd_bearish_divergence(df, macd_divergence_lookback, macd_divergence_min_points)
        c6 = True if not enable_j_hook else self._hook_to_large_negative(df, j_threshold)
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "price": price,
            "pct_chg": pct,
            "vol_today": vol_today,
            "vol_min20": vol_min,
            "amp": amp_val,
            "ma5": ma5,
            "ma10": ma10,
            "kdj_j": j,
            "macd_hist": macd_hist,
            "cond_pct_range": c1,
            "cond_vol_low": c2,
            "cond_amp_limit": c3,
            "cond_above_ma": c4,
            "cond_no_macd_bearish_divergence": c5,
            "cond_j_hook": c6,
            "passed": c1 and c2 and c3 and c4 and c5 and c6
        }
