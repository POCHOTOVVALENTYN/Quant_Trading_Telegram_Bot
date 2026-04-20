from typing import Any, Dict, Optional

import pandas as pd

from core.strategies._base import BaseStrategy


class StrategyMATrend(BaseStrategy):
    """Closed-bar EMA trend following with trend and volatility filters."""

    def __init__(
        self,
        fast_ma: int = 20,
        slow_ma: int = 50,
        global_ma: int = 200,
        long_rsi_max: float = 70.0,
        short_rsi_min: float = 30.0,
    ):
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma
        self.global_ma = global_ma
        self.long_rsi_max = long_rsi_max
        self.short_rsi_min = short_rsi_min

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        fast_col = f"ema{self.fast_ma}"
        slow_col = f"ema{self.slow_ma}"
        global_col = f"ema{self.global_ma}"
        if len(df) < 2 or not self._has_columns(df, fast_col, slow_col, "close"):
            return None

        prev_row, last_row = df.iloc[-2], df.iloc[-1]
        fast_curr = self._safe_float(last_row.get(fast_col))
        slow_curr = self._safe_float(last_row.get(slow_col))
        fast_prev = self._safe_float(prev_row.get(fast_col))
        slow_prev = self._safe_float(prev_row.get(slow_col))
        if min(fast_curr, slow_curr, fast_prev, slow_prev) <= 0:
            return None

        is_cross_up = fast_prev <= slow_prev and fast_curr > slow_curr
        is_cross_down = fast_prev >= slow_prev and fast_curr < slow_curr
        has_global = global_col in df.columns and not pd.isna(last_row.get(global_col))
        global_v = self._safe_float(last_row.get(global_col), self._safe_float(last_row.get("close")))
        rsi = self._safe_float(last_row.get("RSI_fast"), 50.0)

        if not self._volatility_filter(last_row):
            return None

        if is_cross_up and self._trend_filter(last_row, "LONG") and last_row["close"] > fast_curr and rsi <= self.long_rsi_max:
            if not has_global or last_row["close"] >= global_v:
                return self._signal("MA Trend", "LONG", last_row["close"], 0.80 if has_global else 0.70)
        if is_cross_down and self._trend_filter(last_row, "SHORT") and last_row["close"] < fast_curr and rsi >= self.short_rsi_min:
            if not has_global or last_row["close"] <= global_v:
                return self._signal("MA Trend", "SHORT", last_row["close"], 0.80 if has_global else 0.70)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        fast_col = f"ema{self.fast_ma}"
        slow_col = f"ema{self.slow_ma}"
        if len(df) < 2 or not self._has_columns(df, fast_col, slow_col, "close"):
            return None
        prev_row, last_row = df.iloc[-2], df.iloc[-1]
        side = str(side or "").upper()
        if side == "LONG" and prev_row[fast_col] >= prev_row[slow_col] and last_row[fast_col] < last_row[slow_col]:
            return self._exit("ma_trend_crossdown", last_row["close"])
        if side == "SHORT" and prev_row[fast_col] <= prev_row[slow_col] and last_row[fast_col] > last_row[slow_col]:
            return self._exit("ma_trend_crossup", last_row["close"])
        return None


class StrategyPullback(BaseStrategy):
    """Trend pullback using only closed bars, without repainting intrabar touches."""

    def __init__(self, ma_period: int = 20, global_period: int = 200):
        self.ma_period = ma_period
        self.global_period = global_period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        ma_col = f"ema{self.ma_period}"
        global_col = f"ema{self.global_period}"
        if len(df) < max(3, self.global_period) or not self._has_columns(df, ma_col, global_col, "close", "high", "low"):
            return None

        prev_row, last_row = df.iloc[-2], df.iloc[-1]
        ma_v = self._safe_float(last_row.get(ma_col))
        global_v = self._safe_float(last_row.get(global_col))
        if min(ma_v, global_v) <= 0 or not self._volatility_filter(last_row):
            return None

        vol_curr = self._safe_float(last_row.get("volume"))
        vol_avg = self._safe_float(last_row.get("vol_ma20"), vol_curr or 1.0)
        vol_confirm = vol_avg <= 0 or vol_curr < (vol_avg * 1.5)

        if (
            last_row["close"] > global_v
            and prev_row["low"] <= prev_row[ma_col]
            and last_row["close"] > prev_row["close"]
            and vol_confirm
            and self._trend_filter(last_row, "LONG")
        ):
            return self._signal("Pullback", "LONG", last_row["close"], 0.75)

        if (
            last_row["close"] < global_v
            and prev_row["high"] >= prev_row[ma_col]
            and last_row["close"] < prev_row["close"]
            and vol_confirm
            and self._trend_filter(last_row, "SHORT")
        ):
            return self._signal("Pullback", "SHORT", last_row["close"], 0.75)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        ma_col = f"ema{self.ma_period}"
        if len(df) < 1 or ma_col not in df.columns:
            return None
        last = df.iloc[-1]
        close = self._safe_float(last.get("close"))
        ma_v = self._safe_float(last.get(ma_col))
        side = str(side or "").upper()
        if side == "LONG" and close < ma_v:
            return self._exit("pullback_lost_ma_support", close)
        if side == "SHORT" and close > ma_v:
            return self._exit("pullback_lost_ma_resistance", close)
        return None
