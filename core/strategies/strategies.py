from core.strategies._base import BaseStrategy
from core.strategies.breakout import StrategyDonchian, StrategyVolContraction
from core.strategies.mean_reversion import (
    StrategyFundingSqueeze,
    StrategyWideRangeReversal,
    StrategyWilliamsR,
)
from core.strategies.momentum import StrategyWRD
from core.strategies.trend_following import StrategyMATrend, StrategyPullback


class StrategyRuleOf7:
    """
    Rule of 7 (Schwager): расчёт целей на основе диапазона паттерна.
    """

    @staticmethod
    def calculate_targets(high: float, low: float, direction: str = "LONG"):
        rng = high - low
        if rng <= 0:
            return {}

        if direction == "LONG":
            t1 = high + (rng * 0.5)
            t2 = high + (rng * 1.0)
            t3 = high + (rng * 1.5)
        else:
            t1 = low - (rng * 0.5)
            t2 = low - (rng * 1.0)
            t3 = low - (rng * 1.5)

        return {
            "Target 1 (0.5x)": round(t1, 4),
            "Target 2 (1.0x)": round(t2, 4),
            "Target 3 (1.5x)": round(t3, 4),
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
