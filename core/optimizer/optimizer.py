from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

import pandas as pd

from ai.backtest import BacktestEngine
from core.optimizer.evaluator import EvaluationResult, StrategyEvaluator
from core.optimizer.mutator import StrategyMutator, StrategyVariant
from core.optimizer.selector import StrategySelector


@dataclass(frozen=True)
class OptimizationConfig:
    variant_count: int = 10
    top_n: int = 1
    initial_balance: float = 10000.0
    leverage: int = 10
    risk_per_trade_pct: float = 0.05
    score_threshold: float = 0.55
    ai_threshold: float = 0.55


class StrategyOptimizer:
    """
    Loop:
    FOR each strategy:
      generate variants
      backtest
      evaluate
      select best
    """

    DEFAULT_STRATEGY_PARAMS: Dict[str, Dict[str, Any]] = {
        "StrategyDonchian": {"period": 20},
        "StrategyWRD": {"atr_multiplier": 1.6},
        "StrategyMATrend": {"fast_ma": 20, "slow_ma": 50, "global_ma": 200, "long_rsi_max": 70.0, "short_rsi_min": 30.0},
        "StrategyPullback": {"ma_period": 20, "global_period": 200},
        "StrategyVolContraction": {"lookback": 300, "contraction_ratio": 0.6},
        "StrategyWideRangeReversal": {},
        "StrategyWilliamsR": {"overbought": -20.0, "oversold": -80.0, "long_rsi_max": 45.0, "short_rsi_min": 55.0},
        "StrategyFundingSqueeze": {"funding_threshold": 0.015, "short_rsi_reentry": 70.0, "long_rsi_reentry": 30.0},
        "StrategyRuleOf7": {"bars": 7},
    }

    def __init__(
        self,
        *,
        mutator: Optional[StrategyMutator] = None,
        evaluator: Optional[StrategyEvaluator] = None,
        selector: Optional[StrategySelector] = None,
        config: Optional[OptimizationConfig] = None,
    ):
        self.mutator = mutator or StrategyMutator()
        self.evaluator = evaluator or StrategyEvaluator()
        self.selector = selector or StrategySelector()
        self.config = config or OptimizationConfig()

    def optimize(
        self,
        *,
        df: pd.DataFrame,
        strategy_classes: List[Type],
        base_risk_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, EvaluationResult]:
        base_risk = base_risk_params or {"sl_multiplier": 2.0, "tp_multiplier": 3.0}
        best_per_strategy: Dict[str, EvaluationResult] = {}

        for strategy_cls in strategy_classes:
            strategy_key = strategy_cls.__name__
            strategy_params = dict(self.DEFAULT_STRATEGY_PARAMS.get(strategy_key, {}))
            variants = self.mutator.generate_variants(
                strategy_cls=strategy_cls,
                base_strategy_params=strategy_params,
                base_risk_params=base_risk,
                count=self.config.variant_count,
            )

            evaluations: List[EvaluationResult] = []
            for variant in variants:
                backtest_result = self._backtest_variant(df=df, variant=variant)
                evaluations.append(
                    self.evaluator.evaluate(
                        strategy_name=variant.strategy_name,
                        parameters=variant.as_parameters(),
                        backtest_result=backtest_result,
                    )
                )

            best_per_strategy[variant.strategy_name] = self.selector.select(evaluations, top_n=self.config.top_n)[0]

        return best_per_strategy

    def _backtest_variant(self, *, df: pd.DataFrame, variant: StrategyVariant) -> Dict[str, Any]:
        engine = BacktestEngine(
            initial_balance=self.config.initial_balance,
            leverage=self.config.leverage,
            sl_multiplier=float(variant.risk_params.get("sl_multiplier", 2.0)),
            tp_multiplier=float(variant.risk_params.get("tp_multiplier", 3.0)),
            risk_per_trade_pct=self.config.risk_per_trade_pct,
            score_threshold=self.config.score_threshold,
            ai_threshold=self.config.ai_threshold,
        )
        engine.strategies = [variant.strategy_cls(**variant.strategy_params)]
        return engine.run(df.copy())
