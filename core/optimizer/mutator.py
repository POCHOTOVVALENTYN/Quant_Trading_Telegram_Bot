import random
from dataclasses import dataclass
from inspect import signature
from typing import Any, Dict, List, Tuple, Type


@dataclass(frozen=True)
class StrategyVariant:
    strategy_name: str
    strategy_cls: Type
    strategy_params: Dict[str, Any]
    risk_params: Dict[str, Any]

    def as_parameters(self) -> Dict[str, Any]:
        return {
            "strategy_params": dict(self.strategy_params),
            "risk_params": dict(self.risk_params),
        }


class StrategyMutator:
    """
    Generates strategy variations with emphasis on RSI / EMA / SL/TP mutations.
    """

    _PARAM_BOUNDS: Dict[str, Tuple[float, float, str]] = {
        "period": (5, 60, "int"),
        "fast_ma": (5, 50, "int"),
        "slow_ma": (20, 120, "int"),
        "global_ma": (100, 300, "int"),
        "ma_period": (5, 60, "int"),
        "global_period": (100, 300, "int"),
        "long_rsi_max": (35, 80, "float"),
        "short_rsi_min": (20, 65, "float"),
        "short_rsi_reentry": (55, 85, "float"),
        "long_rsi_reentry": (15, 45, "float"),
        "oversold": (-95, -60, "float"),
        "overbought": (-40, -5, "float"),
        "funding_threshold": (0.005, 0.03, "float"),
        "atr_multiplier": (1.0, 3.0, "float"),
        "contraction_ratio": (0.3, 0.9, "float"),
        "lookback": (50, 400, "int"),
        "bars": (3, 21, "int"),
        "std_dev": (1.5, 3.5, "float"),
        "sl_multiplier": (1.0, 4.0, "float"),
        "tp_multiplier": (1.2, 6.0, "float"),
    }

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)

    def generate_variants(
        self,
        *,
        strategy_cls: Type,
        base_strategy_params: Dict[str, Any],
        base_risk_params: Dict[str, Any],
        count: int = 10,
    ) -> List[StrategyVariant]:
        variants = [
            StrategyVariant(
                strategy_name=self._strategy_name(strategy_cls),
                strategy_cls=strategy_cls,
                strategy_params=self._filter_supported_params(strategy_cls, base_strategy_params),
                risk_params=dict(base_risk_params),
            )
        ]

        for _ in range(max(0, count - 1)):
            variants.append(
                StrategyVariant(
                    strategy_name=self._strategy_name(strategy_cls),
                    strategy_cls=strategy_cls,
                    strategy_params=self._mutate_strategy_params(strategy_cls, base_strategy_params),
                    risk_params=self._mutate_risk_params(base_risk_params),
                )
            )
        return variants

    def _mutate_strategy_params(self, strategy_cls: Type, params: Dict[str, Any]) -> Dict[str, Any]:
        supported = self._filter_supported_params(strategy_cls, params)
        mutated = {}
        for key, value in supported.items():
            mutated[key] = self._mutate_value(key, value)
        if strategy_cls.__name__ == "StrategyMATrend":
            if int(mutated.get("slow_ma", 0)) <= int(mutated.get("fast_ma", 0)):
                mutated["slow_ma"] = int(mutated.get("fast_ma", 20)) + 10
        return mutated

    def _mutate_risk_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {key: self._mutate_value(key, value) for key, value in params.items()}

    def _mutate_value(self, key: str, value: Any) -> Any:
        bounds = self._PARAM_BOUNDS.get(key)
        if not bounds:
            return value

        min_v, max_v, value_type = bounds
        base = float(value)
        if value_type == "int":
            spread = max(1.0, abs(base) * 0.35)
            candidate = round(base + self._rng.uniform(-spread, spread))
            return int(max(min_v, min(max_v, candidate)))

        spread = max(0.05, abs(base) * 0.25)
        candidate = base + self._rng.uniform(-spread, spread)
        candidate = max(min_v, min(max_v, candidate))
        return round(candidate, 4)

    @staticmethod
    def _strategy_name(strategy_cls: Type) -> str:
        mapping = {
            "StrategyDonchian": "Donchian",
            "StrategyWRD": "WRD",
            "StrategyMATrend": "MA Trend",
            "StrategyPullback": "Pullback",
            "StrategyVolContraction": "Vol Contraction",
            "StrategyWideRangeReversal": "WRD Reversal",
            "StrategyWilliamsR": "Williams R",
            "StrategyFundingSqueeze": "Funding Squeeze",
            "StrategyRuleOf7": "Rule of 7",
            "StrategyBollingerMR": "BB Mean Reversion",
            "StrategyFakeout": "Fakeout",
        }
        return mapping.get(strategy_cls.__name__, strategy_cls.__name__)

    @staticmethod
    def _filter_supported_params(strategy_cls: Type, params: Dict[str, Any]) -> Dict[str, Any]:
        allowed = set(signature(strategy_cls.__init__).parameters.keys()) - {"self"}
        return {k: v for k, v in params.items() if k in allowed}
