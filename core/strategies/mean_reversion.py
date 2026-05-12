from typing import Any, Dict, Optional

import pandas as pd

from core.strategies._base import BaseStrategy


class StrategyWideRangeReversal(BaseStrategy):
    """Wide range reversal on closed bars only, with contrarian trend and volatility filters."""

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 5 or not self._has_columns(df, "open", "high", "low", "close", "atr"):
            return None

        prev, curr = df.iloc[-2], df.iloc[-1]
        prev_range = self._safe_float(prev["high"] - prev["low"])
        prev_atr = self._safe_float(prev.get("atr"))
        if prev_range <= 0 or prev_atr <= 0 or prev_range < (1.5 * prev_atr):
            return None

        body_ratio = abs(prev["close"] - prev["open"]) / prev_range
        vol_curr = self._safe_float(curr.get("volume"))
        vol_prev = self._safe_float(prev.get("volume"), vol_curr)
        vol_ok = vol_prev <= 0 or vol_curr > vol_prev * 0.8
        if body_ratio <= 0.4 or not vol_ok or not self._volatility_filter(curr):
            return None

        if prev["close"] < (prev["low"] + prev_range * 0.25) and curr["close"] > prev["high"]:
            if self._trend_filter(curr, "LONG", contrarian=True):
                return self._signal("WRD Reversal", "LONG", curr["close"], 0.82)

        if prev["close"] > (prev["high"] - prev_range * 0.25) and curr["close"] < prev["low"]:
            if self._trend_filter(curr, "SHORT", contrarian=True):
                return self._signal("WRD Reversal", "SHORT", curr["close"], 0.82)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 1:
            return None
        last = df.iloc[-1]
        mid = (self._safe_float(last.get("high")) + self._safe_float(last.get("low"))) / 2.0
        close = self._safe_float(last.get("close"))
        side = str(side or "").upper()
        if side == "LONG" and close < mid:
            return self._exit("wrd_reversal_failed_followthrough", close)
        if side == "SHORT" and close > mid:
            return self._exit("wrd_reversal_failed_followthrough", close)
        return None


class StrategyWilliamsR(BaseStrategy):
    """Williams %R mean reversion with contrarian trend and volatility filters."""

    def __init__(
        self,
        overbought: float = -20,
        oversold: float = -80,
        long_rsi_max: float = 45.0,
        short_rsi_min: float = 55.0,
    ):
        self.overbought = overbought
        self.oversold = oversold
        self.long_rsi_max = long_rsi_max
        self.short_rsi_min = short_rsi_min

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 20 or not self._has_columns(df, "williams_r", "RSI_fast", "close"):
            return None

        prev, curr = df.iloc[-2], df.iloc[-1]
        wr_curr = self._safe_float(curr.get("williams_r"))
        wr_prev = self._safe_float(prev.get("williams_r"))
        rsi = self._safe_float(curr.get("RSI_fast"), 50.0)
        if not self._volatility_filter(curr, max_atr_ratio=0.05):
            return None

        if wr_prev <= self.oversold and wr_curr > self.oversold and rsi < self.long_rsi_max:
            if self._trend_filter(curr, "LONG", contrarian=True):
                return self._signal("Williams R", "LONG", curr["close"], 0.75)
        if wr_prev >= self.overbought and wr_curr < self.overbought and rsi > self.short_rsi_min:
            if self._trend_filter(curr, "SHORT", contrarian=True):
                return self._signal("Williams R", "SHORT", curr["close"], 0.75)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 1 or "williams_r" not in df.columns:
            return None
        last = df.iloc[-1]
        wr = self._safe_float(last.get("williams_r"))
        close = self._safe_float(last.get("close"))
        side = str(side or "").upper()
        if side == "LONG" and wr >= -20:
            return self._exit("williams_r_mean_reversion_complete", close)
        if side == "SHORT" and wr <= -80:
            return self._exit("williams_r_mean_reversion_complete", close)
        return None


class StrategyFundingSqueeze(BaseStrategy):
    """Contrarian mean-reversion on extreme funding with closed-bar confirmation."""

    def __init__(
        self,
        funding_threshold: float = 0.015,
        short_rsi_reentry: float = 70.0,
        long_rsi_reentry: float = 30.0,
    ):
        self.funding_threshold = funding_threshold
        self.short_rsi_reentry = short_rsi_reentry
        self.long_rsi_reentry = long_rsi_reentry

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 15 or not self._has_columns(df, "funding_rate", "RSI_fast", "close"):
            return None

        prev, last = df.iloc[-2], df.iloc[-1]
        fr = self._safe_float(last.get("funding_rate"))
        rsi = self._safe_float(last.get("RSI_fast"))
        prev_rsi = self._safe_float(prev.get("RSI_fast"))
        if not self._volatility_filter(last):
            return None

        if fr > self.funding_threshold and prev_rsi >= self.short_rsi_reentry and rsi < self.short_rsi_reentry:
            if self._trend_filter(last, "SHORT", contrarian=True):
                return self._signal("Funding Squeeze", "SHORT", last["close"], 0.80)
        if fr < -self.funding_threshold and prev_rsi <= self.long_rsi_reentry and rsi > self.long_rsi_reentry:
            if self._trend_filter(last, "LONG", contrarian=True):
                return self._signal("Funding Squeeze", "LONG", last["close"], 0.80)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 1:
            return None
        last = df.iloc[-1]
        rsi = self._safe_float(last.get("RSI_fast"))
        close = self._safe_float(last.get("close"))
        side = str(side or "").upper()
        if side == "LONG" and rsi >= 60:
            return self._exit("funding_squeeze_normalized", close)
        if side == "SHORT" and rsi <= 40:
            return self._exit("funding_squeeze_normalized", close)
        return None
