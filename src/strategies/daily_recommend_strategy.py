import pandas as pd
import numpy as np
from src.strategies.base_strategy import BaseStrategy

class ShoufuStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()

    def _is_dual_innovation(self, ts_code: str) -> bool:
        code = ts_code.split('.')[0]
        return code.startswith('300') or code.startswith('688')

    def _has_bearish_divergence(self, df: pd.DataFrame) -> bool:
        recent = df.tail(60).copy()
        if len(recent) < 20:
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

    def _hook_to_large_negative(self, df: pd.DataFrame) -> bool:
        if len(df) < 2:
            return False
        j_prev = df['J'].iloc[-2]
        j_curr = df['J'].iloc[-1]
        return (j_curr <= 0) and (j_curr > j_prev)

    def check_stock(self, ts_code: str, raw_df: pd.DataFrame) -> bool:
        df = self.preprocess(raw_df)
        if len(df) == 0:
            return False
        last = df.iloc[-1]
        pct = last['涨跌幅'] if '涨跌幅' in df.columns else 0.0
        if not (-2.0 <= pct <= 1.8):
            return False
        vol = last['成交量'] if '成交量' in df.columns else 0.0
        vol_min = df['成交量'].tail(20).min() if '成交量' in df.columns else 0.0
        if vol_min == 0:
            return False
        if vol > vol_min * 1.05:
            return False
        amp = self.amplitude(df)
        if self._is_dual_innovation(ts_code):
            if amp >= 7.0:
                return False
        else:
            if amp >= 4.0:
                return False
        if (last['收盘价'] < last['MA5']) or (last['收盘价'] < last['MA10']):
            return False
        if self._has_bearish_divergence(df):
            return False
        if not self._hook_to_large_negative(df):
            return False
        return True

    def explain(self, ts_code: str, raw_df: pd.DataFrame) -> dict:
        df = self.preprocess(raw_df)
        last = df.iloc[-1]
        amp = self.amplitude(df)
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "close": round(last['收盘价'], 2),
            "pct_chg": round(last['涨跌幅'] if '涨跌幅' in df.columns else 0.0, 2),
            "amp": round(amp, 2),
            "above_ma5_ma10": (last['收盘价'] >= last['MA5']) and (last['收盘价'] >= last['MA10']),
            "kdj_j": round(last['J'], 2),
            "macd_hist": round(last['MACD_HIST'], 4)
        }
