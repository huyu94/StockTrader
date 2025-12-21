import pandas as pd
from src.strategies.base_strategy import BaseStrategy

class ShaofuStrategy(BaseStrategy):
    def check_stock(self, ts_code: str, raw_df: pd.DataFrame, j_threshold: float = 5.0) -> bool:
        df = self.preprocess(raw_df)
        if 'J' not in df.columns:
            raise KeyError("缺少必要列：J")
        if len(df) < 2:
            return False
        j_prev = float(df['J'].iloc[-2])
        return j_prev < j_threshold

    def explain(self, ts_code: str, raw_df: pd.DataFrame) -> dict:
        df = self.preprocess(raw_df)
        vol_avg = df['成交量'].tail(20).mean() if '成交量' in df.columns else 0.0
        last = df.iloc[-1]
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "close": float(last['收盘价']) if '收盘价' in df.columns else 0.0,
            "kdj_j": float(last['J']) if 'J' in df.columns else 0.0,
            "macd_hist": float(last['MACD_HIST']) if 'MACD_HIST' in df.columns else 0.0,
            "vol": float(last['成交量']) if '成交量' in df.columns else 0.0,
            "vol_avg20": float(vol_avg)
        }

    def debug_check(self, ts_code: str, raw_df: pd.DataFrame, j_threshold: float = 5.0) -> dict:
        df = self.preprocess(raw_df)
        if 'J' not in df.columns:
            raise KeyError("缺少必要列：J")
        j_prev = float(df['J'].iloc[-2]) if len(df) >= 2 else float('nan')
        j_curr = float(df['J'].iloc[-1])
        c1 = j_prev < j_threshold
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "kdj_j_prev": j_prev,
            "kdj_j": j_curr,
            "cond_j_negative": c1,
            "passed": c1
        }
