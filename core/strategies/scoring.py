import logging
import pandas as pd
from typing import Dict, Any, FrozenSet
from collections import defaultdict

_score_log = logging.getLogger("strategy_scoring")

MEAN_REVERSION_STRATEGIES: FrozenSet[str] = frozenset({
    "Williams R",
    "WRD Reversal",
    "Funding Squeeze",
})

STRATEGY_PRIORITY: Dict[str, float] = {
    "Donchian": 1.15,
    "MA Trend": 1.10,
    "Pullback": 1.10,
    "WRD": 1.05,
    "Vol Contraction": 1.05,
    "Williams R": 1.00,
    "WRD Reversal": 0.95,
    "Funding Squeeze": 0.90,
}

# Dynamic strategy scoring (Phase 5B)
MIN_TRADES_FOR_ADJUSTMENT = 15
PRIORITY_FLOOR = 0.5
PRIORITY_CEILING = 1.5
ROLLING_WINDOW = 30


class DynamicStrategyScorer:
    """
    Adjusts STRATEGY_PRIORITY based on rolling trade performance.
    Tracks last N trades per strategy and scales the priority multiplier.
    """

    def __init__(self):
        self._results: Dict[str, list] = defaultdict(list)  # strategy -> [pnl_usd, ...]

    def record_trade(self, strategy: str, pnl_usd: float):
        """Record a closed trade result for a strategy."""
        results = self._results[strategy]
        results.append(pnl_usd)
        if len(results) > ROLLING_WINDOW:
            self._results[strategy] = results[-ROLLING_WINDOW:]

    def get_adjusted_priority(self, strategy: str) -> float:
        """
        Returns adjusted priority multiplier for a strategy.
        Falls back to static STRATEGY_PRIORITY if insufficient data.
        """
        base = STRATEGY_PRIORITY.get(strategy, 1.0)
        results = self._results.get(strategy, [])
        if len(results) < MIN_TRADES_FOR_ADJUSTMENT:
            return base

        win_rate = sum(1 for r in results if r > 0) / len(results)
        # Scale: 50% WR → 1.0x, 70% → 1.4x, 30% → 0.6x
        adjustment = 0.5 + win_rate
        adjusted = base * adjustment
        clamped = max(PRIORITY_FLOOR, min(PRIORITY_CEILING, adjusted))

        if abs(clamped - base) > 0.05:
            _score_log.info(
                f"Strategy {strategy}: priority {base:.2f} -> {clamped:.2f} "
                f"(WR={win_rate:.1%}, n={len(results)})"
            )
        return clamped

    def get_all_adjustments(self) -> Dict[str, dict]:
        result = {}
        for strategy in STRATEGY_PRIORITY:
            results = self._results.get(strategy, [])
            n = len(results)
            wr = sum(1 for r in results if r > 0) / n if n > 0 else 0
            result[strategy] = {
                "base_priority": STRATEGY_PRIORITY.get(strategy, 1.0),
                "adjusted_priority": self.get_adjusted_priority(strategy),
                "trades": n,
                "win_rate": round(wr, 3),
            }
        return result


class SignalScorer:
    """
    Signal quality scorer (0..1).
    Factors: trend alignment, volatility, volume, momentum.
    Strategy priority multiplier adjusts final score.
    """
    def __init__(self, weights: Dict[str, float] = None, dynamic_scorer: DynamicStrategyScorer = None):
        self.weights = weights or {
            "trend": 0.3,
            "volatility": 0.2,
            "volume": 0.2,
            "momentum": 0.3
        }
        self.dynamic_scorer = dynamic_scorer

    def calculate_score(self, df: pd.DataFrame, signal: Dict[str, Any]) -> float:
        if df.empty or len(df) < 50:
            return 0.0

        last_row = df.iloc[-1]
        direction = signal.get("signal")
        strategy_name = signal.get("strategy") or ""

        trend_score = 0.0
        if strategy_name in MEAN_REVERSION_STRATEGIES:
            trend_score = 0.5
        else:
            trend_col = 'ema50' if 'ema50' in df.columns else None
            if trend_col and not pd.isna(last_row.get(trend_col)):
                if direction == "LONG" and last_row['close'] > last_row[trend_col]:
                    trend_score = 1.0
                elif direction == "SHORT" and last_row['close'] < last_row[trend_col]:
                    trend_score = 1.0
            else:
                sma_50_val = df['close'].tail(50).mean()
                if direction == "LONG" and last_row['close'] > sma_50_val:
                    trend_score = 1.0
                elif direction == "SHORT" and last_row['close'] < sma_50_val:
                    trend_score = 1.0

        vol_score = 0.0
        if 'atr' in df.columns and not pd.isna(last_row.get('atr')):
            atr_ma = df['atr'].tail(10).mean()
            if atr_ma > 0 and last_row['atr'] > atr_ma:
                vol_score = min(1.0, last_row['atr'] / atr_ma - 0.5)

        volu_score = 0.0
        vol_avg = df['volume'].tail(20).mean()
        if vol_avg > 0:
            ratio = last_row['volume'] / vol_avg
            if ratio > 1.5:
                volu_score = 1.0
            elif ratio > 1.0:
                volu_score = 0.5

        mom_score = 0.0
        if len(df) > 10:
            close_10 = df.iloc[-10]['close']
            if close_10 > 0:
                roc = ((last_row['close'] - close_10) / close_10) * 100
                if direction == "LONG" and roc > 0:
                    mom_score = min(1.0, roc / 5.0)
                elif direction == "SHORT" and roc < 0:
                    mom_score = min(1.0, abs(roc) / 5.0)

        raw_score = (
            trend_score * self.weights["trend"] +
            vol_score * self.weights["volatility"] +
            volu_score * self.weights["volume"] +
            mom_score * self.weights["momentum"]
        )

        if self.dynamic_scorer:
            priority = self.dynamic_scorer.get_adjusted_priority(strategy_name)
        else:
            priority = STRATEGY_PRIORITY.get(strategy_name, 1.0)
        return round(min(1.0, raw_score * priority), 4)
