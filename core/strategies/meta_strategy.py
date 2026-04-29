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
        "BB Mean Reversion",
        "Fakeout",
    })

    def __init__(self, adx_trend_min: float = 22.0, adx_flat_max: float = 18.0):
        self.adx_trend_min = float(adx_trend_min)
        self.adx_flat_max = float(adx_flat_max)

    def detect_market_regime(self, df: pd.DataFrame) -> str:
        if df.empty:
            return "NEUTRAL"

        last = df.iloc[-1]
        adx = self._safe_float(last.get("adx"))
        close = self._safe_float(last.get("close"))
        ema50 = self._safe_float(last.get("ema50"), close)
        ema200 = self._safe_float(last.get("ema200"), ema50)

        # 1. Clear TREND (Strict alignment)
        if adx >= self.adx_trend_min:
            aligned_up = close >= ema50 >= ema200
            aligned_down = close <= ema50 <= ema200
            if aligned_up or aligned_down:
                return "TREND"

        # 2. CLEAR RANGE (Very low ADX)
        if adx <= self.adx_flat_max:
            return "RANGE"

        # 3. NEUTRAL (Everything else)
        return "NEUTRAL"

    def select_strategies(self, df: pd.DataFrame, strategies: Iterable[Any]) -> MetaSelection:
        regime = self.detect_market_regime(df)
        strategies = list(strategies)

        if regime == "TREND":
            # In trend, we focus on trend followers
            selected = [s for s in strategies if self._strategy_name(s) in self.TREND_STRATEGIES]
        elif regime == "RANGE":
            # In clear range, we focus on mean reversion
            selected = [s for s in strategies if self._strategy_name(s) in self.FLAT_STRATEGIES]
        else:
            # In NEUTRAL/Messy market, we allow a MIX of robust strategies from both groups
            robust_mix = {"Williams R", "Pullback", "Donchian", "MA Trend"}
            selected = [s for s in strategies if self._strategy_name(s) in robust_mix]

        if not selected:
            selected = strategies

        # Map internal 'RANGE' back to 'FLAT' for external compatibility if needed, 
        # but let's keep 'NEUTRAL' and 'TREND' as is.
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
