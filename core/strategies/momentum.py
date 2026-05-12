from typing import Any, Dict, Optional

import pandas as pd

from core.strategies._base import BaseStrategy


class StrategyWRD(BaseStrategy):
    """Wide Range Day momentum on closed bars only with trend/volatility filters."""

    def __init__(self, atr_multiplier: float = 1.6):
        self.atr_multiplier = atr_multiplier

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 20 or not self._has_columns(df, "open", "high", "low", "close", "atr"):
            return None

        last = df.iloc[-1]
        atr = self._safe_float(last.get("atr"))
        bar_range = self._safe_float(last.get("high") - last.get("low"))
        if atr <= 0 or bar_range <= 0 or not self._volatility_filter(last):
            return None
        if bar_range < self.atr_multiplier * atr:
            return None

        body = self._safe_float(last.get("close") - last.get("open"))
        body_ratio = abs(body) / bar_range
        if body_ratio < 0.5:
            return None

        prev_ranges = (df["high"] - df["low"]).iloc[-4:-1]
        avg_prev_range = prev_ranges.mean()
        if pd.isna(avg_prev_range) or avg_prev_range <= 0 or bar_range < (1.2 * avg_prev_range):
            return None

        conf = min(0.85, 0.6 + (bar_range / atr - self.atr_multiplier) * 0.1)
        if body > 0 and self._trend_filter(last, "LONG"):
            return self._signal("WRD", "LONG", last["close"], conf)
        if body < 0 and self._trend_filter(last, "SHORT"):
            return self._signal("WRD", "SHORT", last["close"], conf)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 1:
            return None
        last = df.iloc[-1]
        close = self._safe_float(last.get("close"))
        open_ = self._safe_float(last.get("open"))
        bar_mid = (self._safe_float(last.get("high")) + self._safe_float(last.get("low"))) / 2.0
        side = str(side or "").upper()
        if side == "LONG" and close < min(open_, bar_mid):
            return self._exit("wrd_momentum_failed", close)
        if side == "SHORT" and close > max(open_, bar_mid):
            return self._exit("wrd_momentum_failed", close)
        return None
