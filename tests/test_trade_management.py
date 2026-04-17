"""Tests for trade management (loss minimization) logic."""

import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from core.risk.risk_manager import RiskManager


# ─── RiskManager static helpers ───

class TestRiskManagerHelpers:
    def test_break_even_price_long(self):
        be = RiskManager.break_even_price(100.0, "LONG", buffer_pct=0.0004)
        assert be == pytest.approx(100.04, abs=0.01)

    def test_break_even_price_short(self):
        be = RiskManager.break_even_price(100.0, "SHORT", buffer_pct=0.0004)
        assert be == pytest.approx(99.96, abs=0.01)

    def test_risk_unit(self):
        assert RiskManager.risk_unit(100, 98) == pytest.approx(2.0)
        assert RiskManager.risk_unit(100, 102) == pytest.approx(2.0)
        assert RiskManager.risk_unit(0, 98) == 0.0

    def test_current_r_multiple_long_profit(self):
        r = RiskManager.current_r_multiple(100, 98, 104, "LONG")
        assert r == pytest.approx(2.0)

    def test_current_r_multiple_long_loss(self):
        r = RiskManager.current_r_multiple(100, 98, 97, "LONG")
        assert r == pytest.approx(-1.5)

    def test_current_r_multiple_short_profit(self):
        r = RiskManager.current_r_multiple(100, 102, 96, "SHORT")
        assert r == pytest.approx(2.0)

    def test_current_r_multiple_short_loss(self):
        r = RiskManager.current_r_multiple(100, 102, 103, "SHORT")
        assert r == pytest.approx(-1.5)

    def test_favorable_bar_confirmed_long(self):
        bar = {"open": 100, "close": 105, "high": 106, "low": 99}
        assert RiskManager.favorable_bar_confirmed(bar, "LONG") is True

    def test_favorable_bar_not_confirmed_long_doji(self):
        bar = {"open": 100, "close": 100.1, "high": 105, "low": 95}
        assert RiskManager.favorable_bar_confirmed(bar, "LONG") is False

    def test_favorable_bar_confirmed_short(self):
        bar = {"open": 105, "close": 100, "high": 106, "low": 99}
        assert RiskManager.favorable_bar_confirmed(bar, "SHORT") is True

    def test_adverse_bar_strong(self):
        bar = {"open": 100, "close": 98, "high": 101, "low": 97}
        assert RiskManager.adverse_bar_is_strong(bar, "LONG", atr=2.0) is True

    def test_adverse_bar_not_strong_small_body(self):
        bar = {"open": 100, "close": 99.5, "high": 101, "low": 99}
        assert RiskManager.adverse_bar_is_strong(bar, "LONG", atr=2.0) is False


# ─── ExecutionEngine trade management ───

def _make_engine():
    """Create a minimal ExecutionEngine with mocked exchange."""
    with patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "test:token"}, clear=False):
        from core.execution.engine import ExecutionEngine
    exchange = MagicMock()
    exchange.market = MagicMock(return_value={"limits": {"amount": {"min": 0.001}, "cost": {"min": 1}}})
    rm = RiskManager()
    engine = ExecutionEngine(exchange, rm)
    return engine


def _base_trade(**overrides):
    defaults = {
        "entry": 100.0,
        "stop": 98.0,
        "initial_stop": 98.0,
        "signal_type": "LONG",
        "opened_at": time.time() - 3600,
        "current_size": 1.0,
        "position_db_id": 1,
        "stop_order_id": "sl-1",
        "tp_order_id": "tp-1",
        "take_profit_live": 106.0,
        "be_moved": False,
        "be_armed": False,
        "partial_done": False,
        "max_favorable_r": 0.0,
        "max_adverse_r": 0.0,
        "bars_since_entry": 0,
        "last_mgmt_bar_ts": 0.0,
        "timeframe": "1h",
        "strategy": "Donchian",
        "setup_group": "breakout",
        "breakout_level": 105.0,
        "invalidation_level": 95.0,
        "ma_at_entry": {"ema20": 99.0, "ema50": 97.0},
        "stage": 0,
    }
    defaults.update(overrides)
    return defaults


class TestTimeStop:
    def test_no_exit_within_soft_limit(self):
        engine = _make_engine()
        trade = _base_trade(opened_at=time.time() - 3600 * 5)
        assert engine._tm_should_time_stop(trade, current_price=101.0) is False

    def test_exit_after_soft_limit_no_profit(self):
        engine = _make_engine()
        trade = _base_trade(
            opened_at=time.time() - 3600 * 30,
            setup_group="breakout",
        )
        assert engine._tm_should_time_stop(trade, current_price=100.0) is True

    def test_no_exit_after_soft_limit_with_profit(self):
        engine = _make_engine()
        trade = _base_trade(
            opened_at=time.time() - 3600 * 30,
            setup_group="breakout",
        )
        assert engine._tm_should_time_stop(trade, current_price=102.0) is False

    def test_hard_limit_always_exits(self):
        engine = _make_engine()
        trade = _base_trade(
            opened_at=time.time() - 3600 * 200,
        )
        assert engine._tm_should_time_stop(trade, current_price=110.0) is True


class TestInvalidationExit:
    def test_breakout_invalidated_long(self):
        engine = _make_engine()
        trade = _base_trade(
            setup_group="breakout",
            invalidation_level=95.0,
            signal_type="LONG",
        )
        assert engine._tm_should_invalidation_exit(trade, current_price=94.0) is True

    def test_breakout_not_invalidated_long(self):
        engine = _make_engine()
        trade = _base_trade(
            setup_group="breakout",
            invalidation_level=95.0,
            signal_type="LONG",
        )
        assert engine._tm_should_invalidation_exit(trade, current_price=96.0) is False

    def test_breakout_invalidated_short(self):
        engine = _make_engine()
        trade = _base_trade(
            setup_group="breakout",
            invalidation_level=105.0,
            signal_type="SHORT",
            entry=100.0,
            stop=102.0,
            initial_stop=102.0,
        )
        assert engine._tm_should_invalidation_exit(trade, current_price=106.0) is True

    def test_mean_reversion_invalidated_deep_loss(self):
        engine = _make_engine()
        trade = _base_trade(
            setup_group="mean_reversion",
            bars_since_entry=10,
        )
        assert engine._tm_should_invalidation_exit(trade, current_price=97.5) is True

    def test_mean_reversion_not_invalidated_shallow_loss(self):
        engine = _make_engine()
        trade = _base_trade(
            setup_group="mean_reversion",
            bars_since_entry=10,
        )
        assert engine._tm_should_invalidation_exit(trade, current_price=99.5) is False

    def test_disabled_setting(self):
        engine = _make_engine()
        trade = _base_trade(setup_group="breakout", invalidation_level=95.0)
        with patch("core.execution.engine.settings") as mock_settings:
            mock_settings.invalidation_exit_enabled = False
            assert engine._tm_should_invalidation_exit(trade, current_price=90.0) is False


class TestBreakEvenArm:
    def test_arm_at_1r_with_adx(self):
        engine = _make_engine()
        trade = _base_trade(be_armed=False, be_moved=False)
        assert engine._tm_should_arm_break_even(trade, current_r=1.1, adx=25.0) is True

    def test_no_arm_below_1r(self):
        engine = _make_engine()
        trade = _base_trade(be_armed=False, be_moved=False)
        assert engine._tm_should_arm_break_even(trade, current_r=0.8, adx=25.0) is False

    def test_no_arm_if_already_armed(self):
        engine = _make_engine()
        trade = _base_trade(be_armed=True, be_moved=False)
        assert engine._tm_should_arm_break_even(trade, current_r=1.5, adx=25.0) is False

    def test_no_arm_without_adx_when_confirmation_required(self):
        engine = _make_engine()
        trade = _base_trade(be_armed=False, be_moved=False)
        assert engine._tm_should_arm_break_even(trade, current_r=1.5, adx=15.0) is False

    def test_arm_without_confirmation_when_disabled(self):
        engine = _make_engine()
        trade = _base_trade(be_armed=False, be_moved=False)
        with patch("core.execution.engine.settings") as mock_settings:
            mock_settings.be_trigger_r = 1.0
            mock_settings.be_require_confirmation = False
            assert engine._tm_should_arm_break_even(trade, current_r=1.1, adx=None) is True


class TestBarsCalculation:
    def test_bars_since_entry_1h(self):
        engine = _make_engine()
        trade = _base_trade(
            opened_at=time.time() - 3600 * 10,
            timeframe="1h",
        )
        bars = engine._tm_bars_since_entry(trade, "1h")
        assert 9 <= bars <= 11

    def test_bars_since_entry_15m(self):
        engine = _make_engine()
        trade = _base_trade(
            opened_at=time.time() - 900 * 20,
            timeframe="15m",
        )
        bars = engine._tm_bars_since_entry(trade, "15m")
        assert 19 <= bars <= 21
