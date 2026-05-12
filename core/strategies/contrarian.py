from typing import Any, Dict, Optional
import pandas as pd
from core.strategies._base import BaseStrategy

class StrategyFakeout(BaseStrategy):
    """
    Fakeout Breakout Strategy: profits from failed breakouts of key levels.
    """
    def __init__(self, period: int = 20):
        self.period = period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period + 3 or not self._has_columns(df, "high", "low", "close", "atr"):
            return None

        # Levels from the window BEFORE the fakeout attempt
        window = df.iloc[-(self.period + 2):-2]
        prev = df.iloc[-2] # The bar that attempted the breakout
        last = df.iloc[-1] # The bar that confirmed the fakeout
        
        highest = self._safe_float(window["high"].max())
        lowest = self._safe_float(window["low"].min())
        close = self._safe_float(last.get("close"))
        atr = self._safe_float(last.get("atr"))

        if atr <= 0 or not self._volatility_filter(last):
            return None

        # FAKEOUT LONG (Bearish Reversal): Price broke ABOVE high and closed back BELOW
        if prev["high"] > highest and prev["close"] < highest and last["close"] < prev["low"]:
            if self._trend_filter(last, "SHORT", contrarian=True):
                return self._signal("Fakeout", "SHORT", close, 0.82)

        # FAKEOUT SHORT (Bullish Reversal): Price broke BELOW low and closed back ABOVE
        if prev["low"] < lowest and prev["close"] > lowest and last["close"] > prev["high"]:
            if self._trend_filter(last, "LONG", contrarian=True):
                return self._signal("Fakeout", "LONG", close, 0.82)
        
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 5: return None
        last = df.iloc[-1]
        close = self._safe_float(last.get("close"))
        atr = self._safe_float(last.get("atr"))
        side = str(side or "").upper()
        # Exit on momentum stall or 1.5 ATR profit if no trailing
        if side == "LONG" and close < last["low"]:
            return self._exit("fakeout_momentum_lost", close)
        if side == "SHORT" and close > last["high"]:
            return self._exit("fakeout_momentum_lost", close)
        return None
