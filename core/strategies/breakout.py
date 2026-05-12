from typing import Any, Dict, Optional

import pandas as pd

from core.strategies._base import BaseStrategy


class StrategyDonchian(BaseStrategy):
    """Breakout only on closed-candle channel breaks; no intrabar repainting."""

    def __init__(self, period: int = 20):
        self.period = period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period + 2 or not self._has_columns(df, "high", "low", "close", "atr"):
            return None

        last = df.iloc[-1]
        prev_window = df.iloc[-(self.period + 1):-1]
        close = self._safe_float(last.get("close"))
        atr = self._safe_float(last.get("atr"))
        highest = self._safe_float(prev_window["high"].max())
        lowest = self._safe_float(prev_window["low"].min())
        vol_v = self._safe_float(last.get("volume"))
        vol_ma = self._safe_float(last.get("vol_ma20"), vol_v)
        vol_ok = vol_ma <= 0 or vol_v >= vol_ma

        if atr <= 0 or not self._volatility_filter(last):
            return None

        if close > highest and vol_ok and self._trend_filter(last, "LONG"):
            strength = max(0.0, (close - highest) / atr)
            return self._signal("Donchian", "LONG", close, min(0.9, 0.6 + strength * 0.1))
        if close < lowest and vol_ok and self._trend_filter(last, "SHORT"):
            strength = max(0.0, (lowest - close) / atr)
            return self._signal("Donchian", "SHORT", close, min(0.9, 0.6 + strength * 0.1))
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < max(10, self.period // 2) + 1:
            return None
        last = df.iloc[-1]
        lookback = df.iloc[-(max(10, self.period // 2) + 1):-1]
        close = self._safe_float(last.get("close"))
        side = str(side or "").upper()
        if side == "LONG" and close < self._safe_float(lookback["low"].min()):
            return self._exit("donchian_failed_breakout", close)
        if side == "SHORT" and close > self._safe_float(lookback["high"].max()):
            return self._exit("donchian_failed_breakout", close)
        return None


class StrategyVolContraction(BaseStrategy):
    """Volatility squeeze breakout with previous-window levels to avoid look-ahead bias."""

    def __init__(self, lookback: int = 300, contraction_ratio: float = 0.6):
        self.lookback = lookback
        self.contraction_ratio = contraction_ratio

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.lookback or not self._has_columns(df, "high", "low", "close", "atr"):
            return None

        atr_recent = df["atr"].iloc[-20:].mean()
        atr_baseline = df["atr"].iloc[-self.lookback:-20].mean()
        if pd.isna(atr_recent) or pd.isna(atr_baseline) or atr_baseline <= 0:
            return None
        if atr_recent >= atr_baseline * self.contraction_ratio:
            return None

        last = df.iloc[-1]
        close = self._safe_float(last.get("close"))
        highest = self._safe_float(df["high"].iloc[-self.lookback:-1].max())
        lowest = self._safe_float(df["low"].iloc[-self.lookback:-1].min())
        vol_curr = self._safe_float(last.get("volume"))
        vol_ma = self._safe_float(last.get("vol_ma20"), vol_curr)
        vol_spike = vol_ma <= 0 or vol_curr > vol_ma * 1.3

        if not self._volatility_filter(last):
            return None

        if close > highest and vol_spike and self._trend_filter(last, "LONG"):
            return self._signal("Vol Contraction", "LONG", close, 0.85)
        if close < lowest and vol_spike and self._trend_filter(last, "SHORT"):
            return self._signal("Vol Contraction", "SHORT", close, 0.85)
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 21:
            return None
        last = df.iloc[-1]
        side = str(side or "").upper()
        close = self._safe_float(last.get("close"))
        atr = self._safe_float(last.get("atr"))
        ema20 = self._safe_float(last.get("ema20"), close)
        if atr <= 0:
            return None
        if side == "LONG" and close < (ema20 - atr * 0.5):
            return self._exit("vol_contraction_reversal", close)
        if side == "SHORT" and close > (ema20 + atr * 0.5):
            return self._exit("vol_contraction_reversal", close)
        return None
