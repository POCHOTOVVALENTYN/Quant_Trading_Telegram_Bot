from typing import Any, Dict, List, Optional
import pandas as pd
from core.strategies._base import BaseStrategy
from core.strategies.breakout import StrategyDonchian, StrategyVolContraction
from core.strategies.mean_reversion import (
    StrategyFundingSqueeze,
    StrategyWideRangeReversal,
    StrategyWilliamsR,
)
from core.strategies.momentum import StrategyWRD
from core.strategies.trend_following import StrategyMATrend, StrategyPullback


class StrategyRuleOf7(BaseStrategy):
    """
    Rule of 7 (Schwager): Optimized for Crypto H4 Swing.
    Identifies momentum breakouts relative to a 7-bar high/low range.
    Uses ATR and Funding catalysts for target generation.
    """
    def __init__(self, bars: int = 7):
        self.bars = bars

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.bars + 2 or not self._has_columns(df, "high", "low", "close", "atr"):
            return None

        last = df.iloc[-1]
        close = self._safe_float(last.get("close"))
        atr = self._safe_float(last.get("atr"))
        if not self._volatility_filter(last):
            return None
        vol_v = self._safe_float(last.get("volume"))
        vol_ma = self._safe_float(last.get("vol_ma20"), vol_v)
        vol_ok = vol_ma <= 0 or vol_v >= vol_ma * 0.85

        # Lookback at the 7 bars BEFORE the current one
        window = df.iloc[-(self.bars + 1):-1]
        highest_7 = window["high"].max()
        lowest_7 = window["low"].min()

        # Signal logic: Close breaks the 7-bar high/low after a period of trend
        # For LONG: needs to be in a general D1/H4 uptrend (handled by filters)
        if close > highest_7 and vol_ok and self._trend_filter(last, "LONG"):
            # Strength is measured by how clean the breakout is relative to ATR
            strength = (close - highest_7) / (atr + 1e-9)
            if 0.1 < strength < 2.0: # Filter out crazy wicks or tiny nudges
                return self._signal("Rule of 7", "LONG", close, 0.75)

        if close < lowest_7 and vol_ok and self._trend_filter(last, "SHORT"):
            strength = (lowest_7 - close) / (atr + 1e-9)
            if 0.1 < strength < 2.0:
                return self._signal("Rule of 7", "SHORT", close, 0.75)

        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        """Failed breakout: price back inside the prior N-bar range (closed bars)."""
        if len(df) < max(10, self.bars // 2) + 1:
            return None
        last = df.iloc[-1]
        lookback = df.iloc[-(max(10, self.bars // 2) + 1):-1]
        close = self._safe_float(last.get("close"))
        side_u = str(side or "").upper()
        if side_u == "LONG" and close < self._safe_float(lookback["low"].min()):
            return self._exit("rule_of_7_failed_breakout", close)
        if side_u == "SHORT" and close > self._safe_float(lookback["high"].max()):
            return self._exit("rule_of_7_failed_breakout", close)
        return None

    @staticmethod
    def calculate_targets(high: float, low: float, direction: str = "LONG", 
                          atr: float = None, funding_rate: float = 0.0, cvd_bias: float = 0.0):
        """
        Calculates targets with ATR normalization and Funding Boost.
        """
        raw_rng = high - low
        if raw_rng <= 0: return {}
        
        # 1. ATR Normalization
        if atr and raw_rng > 3 * atr:
            rng = 1.5 * atr
        else:
            rng = raw_rng

        # 2. Funding Boost
        m1, m2, m3 = 0.5, 1.0, 1.5
        if direction == "LONG" and funding_rate < -0.01: # Aggressive squeeze
            m2, m3 = 1.3, 2.2
        elif direction == "SHORT" and funding_rate > 0.01:
            m2, m3 = 1.3, 2.2

        if direction == "LONG":
            targets = [high + (rng * m) for m in [m1, m2, m3]]
        else:
            targets = [low - (rng * m) for m in [m1, m2, m3]]

        return {
            "t1": round(targets[0], 6),
            "t2": round(targets[1], 6),
            "t3": round(targets[2], 6),
            "portions": [0.4, 0.3, 0.3]
        }


def get_timeframe_seconds(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == "m":
        return val * 60
    if unit == "h":
        return val * 3600
    if unit == "d":
        return val * 86400
    return val


__all__ = [
    "BaseStrategy",
    "StrategyDonchian",
    "StrategyWRD",
    "StrategyMATrend",
    "StrategyPullback",
    "StrategyVolContraction",
    "StrategyWideRangeReversal",
    "StrategyWilliamsR",
    "StrategyFundingSqueeze",
    "StrategyRuleOf7",
    "get_timeframe_seconds",
]
