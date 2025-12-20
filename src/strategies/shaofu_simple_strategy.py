import pandas as pd
from src.strategies.base_strategy import BaseStrategy

class ShaofuSimpleStrategy(BaseStrategy):
    def check_stock(self, ts_code: str, raw_df: pd.DataFrame, j_threshold: float = 0.0, vol_window: int = 20, shrink_ratio: float = 0.5) -> bool:
        df = self.preprocess(raw_df)
        if 'J' not in df.columns or '成交量' not in df.columns:
            raise KeyError("缺少必要列：J 或 成交量")
        if len(df) < 2:
            return False
        j_curr = df['J'].iloc[-1]
        cond1 = j_curr <= j_threshold
        vol_today = df['成交量'].iloc[-1]
        vol_avg = df['成交量'].tail(vol_window).mean()
        if vol_avg == 0:
            raise ValueError("成交量窗口内均值为0，数据不完整")
        cond2 = vol_today <= vol_avg * shrink_ratio
        return cond1 and cond2

    def explain(self, ts_code: str, raw_df: pd.DataFrame) -> dict:
        df = self.preprocess(raw_df)
        vol_avg = df['成交量'].tail(20).mean() if '成交量' in df.columns else 0.0
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "kdj_j": float(df['J'].iloc[-1]) if 'J' in df.columns else 0.0,
            "vol": float(df['成交量'].iloc[-1]) if '成交量' in df.columns else 0.0,
            "vol_avg20": float(vol_avg)
        }

    def debug_check(self, ts_code: str, raw_df: pd.DataFrame, j_threshold: float = 0.0, vol_window: int = 20, shrink_ratio: float = 0.5) -> dict:
        df = self.preprocess(raw_df)
        if 'J' not in df.columns or '成交量' not in df.columns:
            raise KeyError("缺少必要列：J 或 成交量")
        j_curr = float(df['J'].iloc[-1])
        vol_today = float(df['成交量'].iloc[-1])
        vol_avg = float(df['成交量'].tail(vol_window).mean())
        if vol_avg == 0:
            raise ValueError("成交量窗口内均值为0，数据不完整")
        c1 = j_curr <= j_threshold
        c2 = vol_today <= vol_avg * shrink_ratio
        return {
            "ts_code": ts_code,
            "trade_date": raw_df['trade_date'].iloc[-1].strftime('%Y-%m-%d') if 'trade_date' in raw_df.columns else '',
            "kdj_j": j_curr,
            "vol": vol_today,
            "vol_avg": vol_avg,
            "cond_j_negative": c1,
            "cond_vol_shrink": c2,
            "passed": c1 and c2
        }
