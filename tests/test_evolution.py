import pandas as pd

from core.optimizer.evolution import EvolutionConfig, StrategyEvolutionRunner, composite_oos_score
from core.strategies.strategies import StrategyDonchian


def _ohlcv(n: int = 500) -> pd.DataFrame:
    rows = []
    for i in range(n):
        c = 100.0 + i * 0.01
        rows.append(
            {
                "timestamp": i * 3600000,
                "open": c - 0.1,
                "high": c + 0.5,
                "low": c - 0.5,
                "close": c,
                "volume": 1000.0,
            }
        )
    return pd.DataFrame(rows)


def test_composite_oos_score_weights_sharpe_pf_and_penalizes_dd():
    s = composite_oos_score({"sharpe": 1.0, "max_drawdown_pct": 10.0, "profit_factor": 2.0})
    worse_dd = composite_oos_score({"sharpe": 1.0, "max_drawdown_pct": 20.0, "profit_factor": 2.0})
    assert s > worse_dd


def test_evolution_pipeline_with_stubbed_backtest(monkeypatch):
    df = _ohlcv(500)

    def fake_run(self, df_in):
        return {
            "total_trades": 8,
            "sharpe": 0.5,
            "max_drawdown_pct": 6.0,
            "profit_factor": 1.2,
            "total_pnl": 10.0,
            "win_rate": 0.5,
        }

    from ai.backtest import BacktestEngine

    monkeypatch.setattr(BacktestEngine, "run", fake_run)

    runner = StrategyEvolutionRunner(config=EvolutionConfig(variant_count=4, min_trades_train=1, min_trades_test=1))
    out = runner.evolve_strategy(
        df=df,
        strategy_cls=StrategyDonchian,
        base_strategy_params={"period": 20},
    )

    assert out.get("error") is None
    assert out["variants_evaluated"] >= 1
    assert "best_composite_oos" in out
    assert "top_by_sharpe_oos" in out


def test_evolution_rejects_short_history():
    df = _ohlcv(100)
    runner = StrategyEvolutionRunner(config=EvolutionConfig())
    out = runner.evolve_strategy(
        df=df,
        strategy_cls=StrategyDonchian,
        base_strategy_params={"period": 20},
    )
    assert out.get("error") == "insufficient_bars"
