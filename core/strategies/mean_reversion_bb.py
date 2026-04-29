from typing import Any, Dict, Optional
import pandas as pd
from core.strategies._base import BaseStrategy

class StrategyBollingerMR(BaseStrategy):
    """
    Bollinger Mean Reversion: trades returns to the band after a breakout.
    Best for RANGE markets.
    """
    def __init__(self, period: int = 20, std_dev: float = 2.0):
        self.period = period
        self.std_dev = std_dev

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period + 2 or not self._has_columns(df, "bb_upper", "bb_lower", "close"):
            return None
        
        prev, last = df.iloc[-2], df.iloc[-1]
        close = self._safe_float(last.get("close"))
        
        # LONG: Previous closed below lower band, current closed back inside
        if prev["close"] < prev["bb_lower"] and close > last["bb_lower"]:
            if self._trend_filter(last, "LONG", contrarian=True):
                return self._signal("BB Mean Reversion", "LONG", close, 0.78)

        # SHORT: Previous closed above upper band, current closed back inside
        if prev["close"] > prev["bb_upper"] and close < last["bb_upper"]:
            if self._trend_filter(last, "SHORT", contrarian=True):
                return self._signal("BB Mean Reversion", "SHORT", close, 0.78)
        
        return None

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        if len(df) < 1 or "bb_mid" not in df.columns:
            return None
        last = df.iloc[-1]
        close = self._safe_float(last.get("close"))
        mid = self._safe_float(last.get("bb_mid"))
        side = str(side or "").upper()
        # Exit at mean (mid band)
        if side == "LONG" and close >= mid:
            return self._exit("bb_mr_target_reached", close)
        if side == "SHORT" and close <= mid:
            return self._exit("bb_mr_target_reached", close)
        return None
