import pandas as pd

from core.optimizer.evaluator import StrategyEvaluator
from core.optimizer.mutator import StrategyMutator
from core.optimizer.optimizer import OptimizationConfig, StrategyOptimizer
from core.optimizer.selector import StrategySelector
from core.strategies.strategies import StrategyMATrend, StrategyWilliamsR


def _df(rows: int = 250) -> pd.DataFrame:
    data = []
    for i in range(rows):
        close = 100.0 + i * 0.2
        data.append(
            {
                "timestamp": i,
                "open": close - 0.3,
                "high": close + 0.7,
                "low": close - 0.7,
                "close": close,
                "volume": 1000.0 + i,
            }
        )
    return pd.DataFrame(data)


def test_mutator_generates_rsi_ema_and_sl_tp_variations():
    mutator = StrategyMutator(seed=7)

    variants = mutator.generate_variants(
        strategy_cls=StrategyMATrend,
        base_strategy_params={"fast_ma": 20, "slow_ma": 50, "global_ma": 200, "long_rsi_max": 70.0, "short_rsi_min": 30.0},
        base_risk_params={"sl_multiplier": 2.0, "tp_multiplier": 3.0},
        count=4,
    )

    assert len(variants) == 4
    assert all("fast_ma" in v.strategy_params for v in variants)
    assert all("long_rsi_max" in v.strategy_params for v in variants)
    assert all("sl_multiplier" in v.risk_params for v in variants)
    assert all("tp_multiplier" in v.risk_params for v in variants)
    assert any(v.strategy_params["fast_ma"] != 20 for v in variants[1:])


def test_evaluator_calculates_profit_winrate_and_drawdown():
    evaluator = StrategyEvaluator()

    result = evaluator.evaluate(
        strategy_name="MA Trend",
        parameters={"strategy_params": {"fast_ma": 20}, "risk_params": {"sl_multiplier": 2.0, "tp_multiplier": 3.0}},
        backtest_result={
            "total_pnl": 250.0,
            "win_rate": 0.6,
            "max_drawdown_pct": 8.0,
            "total_trades": 12,
            "sharpe": 1.2,
        },
    )

    assert result.metrics["profit"] == 250.0
    assert result.metrics["winrate"] == 0.6
    assert result.metrics["drawdown"] == 8.0
    assert result.score > 0


def test_selector_chooses_top_strategy():
    evaluator = StrategyEvaluator()
    selector = StrategySelector()
    evaluations = [
        evaluator.evaluate(
            strategy_name="A",
            parameters={},
            backtest_result={"total_pnl": 100.0, "win_rate": 0.5, "max_drawdown_pct": 12.0, "total_trades": 10, "sharpe": 0.5},
        ),
        evaluator.evaluate(
            strategy_name="A",
            parameters={},
            backtest_result={"total_pnl": 220.0, "win_rate": 0.62, "max_drawdown_pct": 7.0, "total_trades": 14, "sharpe": 1.1},
        ),
    ]

    top = selector.select(evaluations, top_n=1)

    assert len(top) == 1
    assert top[0].metrics["profit"] == 220.0


def test_optimizer_runs_generate_backtest_evaluate_select_loop(monkeypatch):
    optimizer = StrategyOptimizer(config=OptimizationConfig(variant_count=3, top_n=1))

    def fake_backtest_variant(self, *, df, variant):
        fast_ma = float(variant.strategy_params.get("fast_ma", 0.0) or 0.0)
        oversold = float(variant.strategy_params.get("oversold", -80.0) or -80.0)
        sl_mult = float(variant.risk_params.get("sl_multiplier", 2.0) or 2.0)
        profit = fast_ma + abs(oversold) - sl_mult * 10.0
        return {
            "total_pnl": profit,
            "win_rate": 0.55,
            "max_drawdown_pct": 5.0,
            "total_trades": 15,
            "sharpe": 1.0,
        }

    monkeypatch.setattr(StrategyOptimizer, "_backtest_variant", fake_backtest_variant)

    results = optimizer.optimize(
        df=_df(),
        strategy_classes=[StrategyMATrend, StrategyWilliamsR],
        base_risk_params={"sl_multiplier": 2.0, "tp_multiplier": 3.0},
    )

    assert "MA Trend" in results
    assert "Williams R" in results
    assert results["MA Trend"].metrics["profit"] > 0
    assert results["Williams R"].metrics["profit"] > 0
