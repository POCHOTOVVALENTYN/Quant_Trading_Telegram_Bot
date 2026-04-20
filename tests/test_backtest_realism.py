import pytest
import pandas as pd

from ai.backtest import BacktestEngine


def test_backtest_entry_fill_uses_open_with_slippage():
    engine = BacktestEngine(entry_slippage_pct=0.001, exit_slippage_pct=0.0, taker_fee_pct=0.0)
    bar = pd.Series({"open": 100.0, "high": 101.0, "low": 99.0})

    long_fill = engine._entry_fill_price(bar, "LONG")
    short_fill = engine._entry_fill_price(bar, "SHORT")

    assert long_fill == 100.1
    assert short_fill == 99.9


def test_backtest_exit_fill_models_gap_and_slippage_for_stop():
    engine = BacktestEngine(entry_slippage_pct=0.0, exit_slippage_pct=0.001, taker_fee_pct=0.0)
    bar = pd.Series({"open": 95.0, "high": 100.0, "low": 94.0})

    long_stop_fill = engine._exit_fill_price(bar, "LONG", 98.0, "SL")
    short_stop_fill = engine._exit_fill_price(pd.Series({"open": 105.0, "high": 106.0, "low": 100.0}), "SHORT", 102.0, "SL")

    assert long_stop_fill == pytest.approx(94.905)
    assert short_stop_fill == pytest.approx(105.105)


def test_backtest_close_trade_includes_fees():
    engine = BacktestEngine(entry_slippage_pct=0.0, exit_slippage_pct=0.0, taker_fee_pct=0.001)
    trade = {
        "entry": 100.0,
        "direction": "LONG",
        "size": 1.0,
        "entry_fee_usd": 0.1,
        "strategy": "Test",
    }

    closed = engine._close_trade(trade, 110.0, "TP")

    assert closed["gross_pnl"] == pytest.approx(10.0)
    assert closed["exit_fee_usd"] == pytest.approx(0.11)
    assert closed["fees_usd"] == pytest.approx(0.21)
    assert closed["pnl"] == pytest.approx(9.79)
