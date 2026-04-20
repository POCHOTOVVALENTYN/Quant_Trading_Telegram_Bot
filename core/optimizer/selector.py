from typing import List

from core.optimizer.evaluator import EvaluationResult


class StrategySelector:
    """Chooses top strategy variants by evaluation score."""

    def select(self, evaluations: List[EvaluationResult], top_n: int = 1) -> List[EvaluationResult]:
        ordered = sorted(
            evaluations,
            key=lambda item: (
                item.score,
                item.metrics.get("profit", 0.0),
                item.metrics.get("winrate", 0.0),
                -item.metrics.get("drawdown", 0.0),
            ),
            reverse=True,
        )
        return ordered[: max(1, int(top_n))]
