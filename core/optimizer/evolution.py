"""
Evolution mode: generate N parameter variants per strategy, backtest on train / OOS,
rank by Sharpe, drawdown, profit factor, and a composite OOS score (anti-overfit).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Type

import pandas as pd

from ai.backtest import BacktestEngine
from core.optimizer.mutator import StrategyMutator, StrategyVariant


@dataclass
class EvolutionConfig:
    """variant_count includes the baseline (first variant is unmutated)."""

    variant_count: int = 10
    train_ratio: float = 0.7
    min_trades_train: int = 3
    min_trades_test: int = 2
    top_per_metric: int = 3
    initial_balance: float = 10000.0
    leverage: int = 10
    risk_per_trade_pct: float = 0.05
    score_threshold: float = 0.55
    ai_threshold: float = 0.55


def _extract_metrics(res: Dict[str, Any]) -> Dict[str, float]:
    pf = float(res.get("profit_factor", 0.0) or 0.0)
    if pf == float("inf") or pf > 1e6:
        pf = 10.0
    return {
        "sharpe": float(res.get("sharpe", 0.0) or 0.0),
        "max_drawdown_pct": float(res.get("max_drawdown_pct", 0.0) or 0.0),
        "profit_factor": pf,
        "total_trades": float(res.get("total_trades", 0) or 0),
        "total_pnl": float(res.get("total_pnl", 0.0) or 0.0),
        "win_rate": float(res.get("win_rate", 0.0) or 0.0),
    }


def composite_oos_score(m_test: Dict[str, float]) -> float:
    """Higher is better; uses OOS (test) metrics only."""
    sharpe = m_test["sharpe"]
    dd = m_test["max_drawdown_pct"]
    pf = m_test["profit_factor"]
    return sharpe * 1.2 + pf * 0.35 - (dd / 100.0) * 0.9


@dataclass
class VariantResult:
    strategy_params: Dict[str, Any]
    risk_params: Dict[str, Any]
    train: Dict[str, float]
    test: Dict[str, float]
    composite_score: float


class StrategyEvolutionRunner:
    """
    1) Split df into train / test (time-ordered OOS).
    2) Build `variant_count` StrategyVariant rows (mutation + baseline).
    3) Backtest each on train and test.
    4) Keep candidates with enough trades on both segments.
    5) Report top-K by Sharpe (test), lowest drawdown (test), profit factor (test),
       and best composite OOS score vs baseline.
    """

    def __init__(
        self,
        *,
        mutator: Optional[StrategyMutator] = None,
        config: Optional[EvolutionConfig] = None,
    ):
        self.mutator = mutator or StrategyMutator()
        self.config = config or EvolutionConfig()

    def _engine(self, risk: Dict[str, Any]) -> BacktestEngine:
        return BacktestEngine(
            initial_balance=self.config.initial_balance,
            leverage=self.config.leverage,
            sl_multiplier=float(risk.get("sl_multiplier", 2.0)),
            tp_multiplier=float(risk.get("tp_multiplier", 3.0)),
            risk_per_trade_pct=self.config.risk_per_trade_pct,
            score_threshold=self.config.score_threshold,
            ai_threshold=self.config.ai_threshold,
        )

    def _run_bt(self, engine: BacktestEngine, df: pd.DataFrame, strategy_cls: Type, params: Dict[str, Any]) -> Dict[str, Any]:
        engine.strategies = [strategy_cls(**params)]
        return engine.run(df.copy())

    def evolve_strategy(
        self,
        *,
        df: pd.DataFrame,
        strategy_cls: Type,
        base_strategy_params: Dict[str, Any],
        base_risk_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        risk_base = dict(base_risk_params or {"sl_multiplier": 2.0, "tp_multiplier": 3.0})
        min_bars = 200  # BacktestEngine.run warmup
        n = len(df)
        if n < 2 * min_bars:
            return {
                "strategy_name": StrategyMutator._strategy_name(strategy_cls),
                "error": "insufficient_bars",
                "need_rows": 2 * min_bars,
                "have_rows": n,
            }
        split = int(n * self.config.train_ratio)
        split = max(min_bars, split)
        split = min(split, n - min_bars)
        train_df = df.iloc[:split].copy()
        test_df = df.iloc[split:].copy()

        variants: List[StrategyVariant] = self.mutator.generate_variants(
            strategy_cls=strategy_cls,
            base_strategy_params=dict(base_strategy_params),
            base_risk_params=risk_base,
            count=self.config.variant_count,
        )

        results: List[VariantResult] = []
        for v in variants:
            eng_tr = self._engine(v.risk_params)
            eng_te = self._engine(v.risk_params)
            res_tr = self._run_bt(eng_tr, train_df, v.strategy_cls, v.strategy_params)
            res_te = self._run_bt(eng_te, test_df, v.strategy_cls, v.strategy_params)
            if res_tr.get("error") or res_te.get("error"):
                continue
            if int(res_tr.get("total_trades", 0) or 0) < self.config.min_trades_train:
                continue
            if int(res_te.get("total_trades", 0) or 0) < self.config.min_trades_test:
                continue
            mt = _extract_metrics(res_tr)
            ms = _extract_metrics(res_te)
            results.append(
                VariantResult(
                    strategy_params=dict(v.strategy_params),
                    risk_params=dict(v.risk_params),
                    train=mt,
                    test=ms,
                    composite_score=composite_oos_score(ms),
                )
            )

        strat_key = StrategyMutator._strategy_name(strategy_cls)

        if not results:
            return {
                "strategy_name": strat_key,
                "error": "no_variants_passed_filters",
                "train_bars": len(train_df),
                "test_bars": len(test_df),
            }

        baseline = results[0]
        best = max(results, key=lambda r: r.composite_score)

        def _as_dict(r: VariantResult) -> Dict[str, Any]:
            return {
                "strategy_params": r.strategy_params,
                "risk_params": r.risk_params,
                "train_metrics": r.train,
                "test_metrics": r.test,
                "composite_oos_score": round(r.composite_score, 6),
            }

        k = max(1, self.config.top_per_metric)
        by_sharpe = sorted(results, key=lambda r: r.test["sharpe"], reverse=True)[:k]
        by_dd = sorted(results, key=lambda r: r.test["max_drawdown_pct"])[:k]
        by_pf = sorted(results, key=lambda r: r.test["profit_factor"], reverse=True)[:k]

        improve = best.composite_score > baseline.composite_score + 1e-6

        return {
            "strategy_name": strat_key,
            "train_bars": len(train_df),
            "test_bars": len(test_df),
            "variants_evaluated": len(results),
            "baseline": _as_dict(baseline),
            "best_composite_oos": _as_dict(best),
            "replace_baseline_recommended": bool(improve),
            "top_by_sharpe_oos": [_as_dict(r) for r in by_sharpe],
            "top_by_drawdown_oos": [_as_dict(r) for r in by_dd],
            "top_by_profit_factor_oos": [_as_dict(r) for r in by_pf],
        }


def run_evolution_suite(
    df: pd.DataFrame,
    specs: List[Tuple[Type, Dict[str, Any]]],
    *,
    config: Optional[EvolutionConfig] = None,
    mutator: Optional[StrategyMutator] = None,
) -> Dict[str, Any]:
    """Run evolution for multiple (strategy_class, base_params) pairs."""
    runner = StrategyEvolutionRunner(mutator=mutator, config=config)
    out: Dict[str, Any] = {}
    for cls, params in specs:
        key = StrategyMutator._strategy_name(cls)  # noqa: SLF001
        out[key] = runner.evolve_strategy(
            df=df,
            strategy_cls=cls,
            base_strategy_params=params,
        )
    return out
