import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.execution.engine import ExecutionEngine
from core.risk.risk_manager import RiskManager


def test_manual_reduce_success_calls_reduce_only_and_reconcile(monkeypatch):
    exchange = MagicMock()
    exchange.create_order = AsyncMock(return_value={"id": "ord-1"})
    exchange.market = MagicMock(return_value={"limits": {"amount": {"min": 0.001}}})

    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine.active_trades["BTC/USDT"] = {"signal_type": "LONG", "current_size": 2.0}
    engine._get_live_position = AsyncMock(return_value=(2.0, "LONG"))
    engine.reconcile_full = AsyncMock()

    monkeypatch.setattr("core.execution.engine.asyncio.sleep", AsyncMock())

    result = asyncio.run(engine.manual_reduce("BTC/USDT", 0.25))

    assert result["status"] == "success"
    exchange.create_order.assert_awaited_once()
    args = exchange.create_order.await_args.args
    assert args[0] == "BTC/USDT"
    assert args[1] == "market"
    assert args[2] == "sell"
    assert args[3] == pytest.approx(0.5)
    assert args[5]["reduceOnly"] is True
    engine.reconcile_full.assert_awaited_once()


def test_manual_reduce_rejects_invalid_fraction():
    exchange = MagicMock()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine.active_trades["BTC/USDT"] = {"signal_type": "LONG", "current_size": 1.0}

    res_zero = asyncio.run(engine.manual_reduce("BTC/USDT", 0.0))
    res_big = asyncio.run(engine.manual_reduce("BTC/USDT", 1.0))

    assert res_zero["status"] == "error"
    assert "Fraction must be" in res_zero["message"]
    assert res_big["status"] == "error"


def test_manual_reduce_rejects_side_mismatch():
    exchange = MagicMock()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine.active_trades["BTC/USDT"] = {"signal_type": "LONG", "current_size": 1.0}
    engine._get_live_position = AsyncMock(return_value=(1.0, "SHORT"))

    result = asyncio.run(engine.manual_reduce("BTC/USDT", 0.25))

    assert result["status"] == "error"
    assert "Side mismatch" in result["message"]


