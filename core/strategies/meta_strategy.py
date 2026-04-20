from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


@dataclass(frozen=True)
class MetaSelection:
    regime: str
    strategies: List[Any]


class MetaStrategy:
    """
    Detects a coarse market regime and selects the relevant strategy bucket.
    Regimes are intentionally simple:
    - TREND: strong directional market
    - FLAT: range / neutral market
    """

    TREND_STRATEGIES = frozenset({
        "Donchian",
        "MA Trend",
        "Pullback",
        "Vol Contraction",
        "WRD",
    })
    FLAT_STRATEGIES = frozenset({
        "Williams R",
        "WRD Reversal",
        "Funding Squeeze",
    })

    def __init__(self, adx_trend_min: float = 22.0, adx_flat_max: float = 18.0):
        self.adx_trend_min = float(adx_trend_min)
        self.adx_flat_max = float(adx_flat_max)

    def detect_market_regime(self, df: pd.DataFrame) -> str:
        if df.empty:
            return "FLAT"

        last = df.iloc[-1]
        adx = self._safe_float(last.get("adx"))
        close = self._safe_float(last.get("close"))
        ema50 = self._safe_float(last.get("ema50"), close)
        ema200 = self._safe_float(last.get("ema200"), ema50)
        atr = self._safe_float(last.get("atr"))

        if adx >= self.adx_trend_min and close > 0 and ema50 > 0 and ema200 > 0:
            aligned_up = close >= ema50 >= ema200
            aligned_down = close <= ema50 <= ema200
            if aligned_up or aligned_down:
                return "TREND"

        # Flat when ADX is explicitly low or when volatility compression is visible.
        if adx <= self.adx_flat_max:
            return "FLAT"
        if close > 0 and atr > 0 and (atr / close) < 0.0035:
            return "FLAT"
        return "FLAT"

    def select_strategies(self, df: pd.DataFrame, strategies: Iterable[Any]) -> MetaSelection:
        regime = self.detect_market_regime(df)
        strategies = list(strategies)

        if regime == "TREND":
            selected = [s for s in strategies if self._strategy_name(s) in self.TREND_STRATEGIES]
        else:
            selected = [s for s in strategies if self._strategy_name(s) in self.FLAT_STRATEGIES]

        # Fail-safe: if mapping misses a strategy, keep the original list instead of silencing signals.
        if not selected:
            selected = strategies

        return MetaSelection(regime=regime, strategies=selected)

    @staticmethod
    def _strategy_name(strategy: Any) -> str:
        try:
            dummy = pd.DataFrame()
            name = getattr(strategy, "strategy_name", None)
            if name:
                return str(name)
            signal = strategy.evaluate(dummy) if hasattr(strategy, "evaluate") else None
            if isinstance(signal, dict) and signal.get("strategy"):
                return str(signal["strategy"])
        except Exception:
            pass

        mapping = {
            "StrategyDonchian": "Donchian",
            "StrategyMATrend": "MA Trend",
            "StrategyPullback": "Pullback",
            "StrategyVolContraction": "Vol Contraction",
            "StrategyWRD": "WRD",
            "StrategyWilliamsR": "Williams R",
            "StrategyWideRangeReversal": "WRD Reversal",
            "StrategyFundingSqueeze": "Funding Squeeze",
        }
        return mapping.get(type(strategy).__name__, type(strategy).__name__)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            val = float(value)
            return default if pd.isna(val) else val
        except (TypeError, ValueError):
            return default
