from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class EvaluationResult:
    strategy_name: str
    parameters: Dict[str, Any]
    metrics: Dict[str, float]
    score: float


class StrategyEvaluator:
    """Turns raw backtest output into comparable optimization metrics."""

    def evaluate(
        self,
        *,
        strategy_name: str,
        parameters: Dict[str, Any],
        backtest_result: Dict[str, Any],
    ) -> EvaluationResult:
        profit = float(backtest_result.get("total_pnl", 0.0) or 0.0)
        winrate = float(backtest_result.get("win_rate", 0.0) or 0.0)
        drawdown = float(backtest_result.get("max_drawdown_pct", 0.0) or 0.0)
        trades = int(backtest_result.get("total_trades", 0) or 0)
        sharpe = float(backtest_result.get("sharpe", 0.0) or 0.0)
        profit_factor = float(backtest_result.get("profit_factor", 0.0) or 0.0)
        if profit_factor == float("inf"):
            profit_factor = 10.0

        metrics = {
            "profit": profit,
            "winrate": winrate,
            "drawdown": drawdown,
            "trades": trades,
            "sharpe": sharpe,
            "profit_factor": profit_factor,
        }
        score = self._score(metrics)
        return EvaluationResult(
            strategy_name=strategy_name,
            parameters=dict(parameters),
            metrics=metrics,
            score=score,
        )

    @staticmethod
    def _score(metrics: Dict[str, float]) -> float:
        profit = float(metrics.get("profit", 0.0))
        winrate = float(metrics.get("winrate", 0.0))
        drawdown = float(metrics.get("drawdown", 0.0))
        sharpe = float(metrics.get("sharpe", 0.0))
        trades = float(metrics.get("trades", 0.0))

        trade_factor = min(1.0, trades / 10.0)
        pf = float(metrics.get("profit_factor", 0.0) or 0.0)
        pf_term = min(pf, 10.0) * 0.5
        return round(
            (profit * 0.01) + (winrate * 100.0) + (sharpe * 10.0) - (drawdown * 1.5) + trade_factor + pf_term,
            4,
        )
