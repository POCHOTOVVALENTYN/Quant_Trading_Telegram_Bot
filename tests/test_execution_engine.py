import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.execution.engine import ExecutionEngine
from core.risk.risk_manager import RiskManager


class _DummySession:
    def __init__(self):
        self.execute = AsyncMock(return_value=SimpleNamespace(rowcount=1))
        self.commit = AsyncMock()
        self.refresh = AsyncMock(side_effect=self._refresh)
        self.add = MagicMock()

    async def _refresh(self, obj):
        if getattr(obj, "id", None) is None:
            obj.id = 123


class _DummySessionCtx:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _SessionFactory:
    def __init__(self):
        self.sessions = []

    def __call__(self):
        s = _DummySession()
        self.sessions.append(s)
        return _DummySessionCtx(s)


@pytest.mark.asyncio
async def test_set_protective_orders_partial_tp_creates_scaled_orders(monkeypatch):
    exchange = MagicMock()
    exchange.request = AsyncMock(
        side_effect=[
            {"algoOrders": []},
            {"algoId": "sl-1"},
            {"algoId": "tp-1"},
            {"algoId": "tp-2"},
            {"algoId": "tp-3"},
        ]
    )
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine._get_position_mode = AsyncMock(return_value=False)
    engine._normalize_price = AsyncMock(side_effect=lambda _s, p: p)
    engine._normalize_amount = AsyncMock(side_effect=lambda _s, a: a)

    tp_targets = {"t1": 101.0, "t2": 102.0, "t3": 103.0}
    sl_id, tp_id = await engine._set_protective_orders("BTC/USDT", "LONG", 1.0, 95.0, tp_targets)

    assert sl_id == "sl-1"
    assert tp_id == "tp-1,tp-2,tp-3"

    # 1 вызов на pre-dedup + 1 SL + 3 частичных TP.
    assert exchange.request.await_count == 5
    _, _, _, sl_payload = exchange.request.await_args_list[1].args
    assert sl_payload["type"] == "STOP_MARKET"
    assert sl_payload["closePosition"] == "true"

    tp_payloads = [call.args[3] for call in exchange.request.await_args_list[2:]]
    assert all(p["type"] == "TAKE_PROFIT_MARKET" for p in tp_payloads)
    assert all("quantity" in p for p in tp_payloads)
    assert all("closePosition" not in p for p in tp_payloads)


@pytest.mark.asyncio
async def test_watch_user_orders_loop_triggers_reconcile_on_ws_error(monkeypatch):
    exchange = MagicMock()
    exchange.watch_orders = AsyncMock(side_effect=Exception("ws down"))
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine._running = True
    engine.reconcile_full = AsyncMock()

    async def _fake_sleep(_sec):
        engine._running = False

    monkeypatch.setattr("core.execution.engine.asyncio.sleep", _fake_sleep)
    await engine._watch_user_orders_loop()

    engine.reconcile_full.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_signal_emergency_close_when_sl_not_created(monkeypatch):
    exchange = MagicMock()
    exchange.fetch_order_book = AsyncMock(side_effect=Exception("no book"))
    exchange.create_order = AsyncMock(
        side_effect=[
            {"average": 100.0, "price": 100.0, "filled": 1.0},  # fallback entry
            {"average": 99.9, "price": 99.9, "filled": 1.0},    # emergency close
        ]
    )
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine._entry_policy_activated = True
    engine._set_leverage_best_effort = AsyncMock()
    engine._normalize_amount = AsyncMock(return_value=1.0)
    engine._set_protective_orders = AsyncMock(return_value=(None, None))
    engine.risk_manager.check_trade_allowed = MagicMock(return_value=True)
    engine.risk_manager.calculate_atr_stop = MagicMock(return_value=95.0)
    engine.risk_manager.calculate_position_size = MagicMock(return_value=1.0)

    session_factory = _SessionFactory()
    monkeypatch.setattr("core.execution.engine.async_session", session_factory)
    monkeypatch.setattr("core.execution.engine.send_telegram_msg", AsyncMock())

    await engine.execute_signal(
        {
            "id": 1,
            "symbol": "BTC/USDT",
            "signal": "LONG",
            "entry_price": 100.0,
            "atr": 2.0,
            "take_profit": 110.0,
            "timeframe": "1h",
        },
        account_balance=1000.0,
        drawdown=0.0,
        open_count=0,
    )

    # Должны быть entry + аварийное закрытие.
    assert exchange.create_order.await_count == 2
    # После аварийного пути EXECUTED не ставится, сигнал должен уйти в FAILED.
    assert session_factory.sessions[-1].execute.await_count == 1


@pytest.mark.asyncio
async def test_execute_signal_waits_until_flat_before_new_policy(monkeypatch):
    exchange = MagicMock()
    exchange.create_order = AsyncMock()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine.risk_manager.check_trade_allowed = MagicMock(return_value=True)
    engine.get_account_metrics = AsyncMock(return_value=(1000.0, 0.0, 1))  # Есть открытая позиция
    engine._set_leverage_best_effort = AsyncMock()
    engine._normalize_amount = AsyncMock(return_value=1.0)
    engine._set_protective_orders = AsyncMock(return_value=("sl-1", "tp-1"))

    session_factory = _SessionFactory()
    monkeypatch.setattr("core.execution.engine.async_session", session_factory)
    monkeypatch.setattr("core.execution.engine.send_telegram_msg", AsyncMock())

    await engine.execute_signal(
        {"id": 1, "symbol": "BTC/USDT", "signal": "LONG", "entry_price": 100.0, "atr": 1.0},
        account_balance=1000.0,
        drawdown=0.0,
        open_count=0,
    )

    # Пока есть открытые позиции, вход не должен стартовать.
    assert engine._entry_policy_activated is False
    exchange.create_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_close_position_side_mismatch_skips_market_close(monkeypatch):
    exchange = MagicMock()
    exchange.cancel_all_orders = AsyncMock()
    exchange.request = AsyncMock()
    exchange.create_order = AsyncMock()

    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine.active_trades["BTC/USDT"] = {
        "signal_type": "LONG",
        "current_size": 1.0,
        "position_db_id": 10,
        "entry": 100.0,
    }
    engine._get_live_position = AsyncMock(return_value=(1.0, "SHORT"))

    await engine._close_position("BTC/USDT", reason="MANUAL")

    # При mismatch не должен отправляться рыночный close.
    exchange.create_order.assert_not_awaited()
    # И запись в памяти не должна удаляться, т.к. закрытие не выполнено.
    assert "BTC/USDT" in engine.active_trades


@pytest.mark.asyncio
async def test_schedule_update_positions_moves_stop_to_breakeven_on_confirmed_trigger(monkeypatch):
    exchange = MagicMock()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine.active_trades["BTC/USDT"] = {
        "entry": 100.0,
        "stop": 95.0,
        "initial_stop": 95.0,
        "be_moved": False,
        "opened_at": 0.0,
        "timeframe": "1m",
        "signal_type": "LONG",
        "current_size": 1.0,
        "position_db_id": 1,
        "take_profit_live": 110.0,
    }
    engine._get_live_position = AsyncMock(return_value=(1.0, "LONG"))
    engine._cancel_all_orders = AsyncMock()
    engine._set_protective_orders = AsyncMock(return_value=("sl-be", "tp-1"))
    engine.time_exit.should_exit = MagicMock(return_value=False)

    session_factory = _SessionFactory()
    monkeypatch.setattr("core.execution.engine.async_session", session_factory)
    monkeypatch.setattr("core.execution.engine.send_telegram_msg", AsyncMock())

    # entry=100, initial_stop=95 => 1R = 5. Триггер 1R выполнен на цене 105.5.
    await engine.schedule_update_positions("BTC/USDT", current_price=105.5, atr=2.0, adx=21.0)

    assert engine.active_trades["BTC/USDT"]["be_moved"] is True
    assert engine.active_trades["BTC/USDT"]["stop"] > 100.0
    engine._set_protective_orders.assert_awaited()


def test_classify_protective_order_without_type_uses_trigger_vs_entry():
    exchange = MagicMock()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())

    # LONG: ниже входа = SL, выше входа = TP
    assert engine._classify_protective_order("", 99.0, 100.0, True) == "SL"
    assert engine._classify_protective_order("", 101.0, 100.0, True) == "TP"

    # SHORT: выше входа = SL, ниже входа = TP
    assert engine._classify_protective_order("", 101.0, 100.0, False) == "SL"
    assert engine._classify_protective_order("", 99.0, 100.0, False) == "TP"


def test_pick_better_level_prefers_closest_valid_level():
    exchange = MagicMock()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())

    # Для LONG стоп должен быть максимально близким к входу снизу.
    sl = engine._pick_better_level(None, 95.0, kind="SL", is_long=True)
    sl = engine._pick_better_level(sl, 96.0, kind="SL", is_long=True)
    assert sl == 96.0

    # Для LONG TP должен быть ближайшим к входу сверху.
    tp = engine._pick_better_level(None, 110.0, kind="TP", is_long=True)
    tp = engine._pick_better_level(tp, 108.0, kind="TP", is_long=True)
    assert tp == 108.0
