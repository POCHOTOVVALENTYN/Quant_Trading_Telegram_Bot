import pytest
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace
import datetime

from core.execution.engine import ExecutionEngine
from core.risk.risk_manager import RiskManager
from core.position.position_manager import PositionManager, PositionState
from database.models.all_models import (
    Position as PositionModel, Order as OrderModel, 
    PositionStatus, OrderStatus
)

# --- 1. Risk Manager Tests ---

def test_calculate_atr_stop():
    rm = RiskManager()
    # LONG: 100 - (1.0 * 2) = 98.
    stop_long = rm.calculate_atr_stop(100.0, atr=1.0, direction="LONG", multiplier=2.0)
    assert stop_long == pytest.approx(98.0)
    
    # SHORT: 100 + (1.0 * 2) = 102.
    stop_short = rm.calculate_atr_stop(100.0, atr=1.0, direction="SHORT", multiplier=2.0)
    assert stop_short == pytest.approx(102.0)

# --- 2. Position Manager Tests ---

def test_position_manager_edge_cases():
    # Very small quantities
    opened = PositionManager.open_position(side="LONG", qty=0.0000001, entry_price=50000.0)
    assert opened.state.qty == 0.0000001
    
    # Negative realized PnL with fees
    reduced = PositionManager.partial_close(
        state=opened.state,
        qty=0.00000005,
        exit_price=40000.0,
        exit_fee_usd=0.01
    )
    # Gross PnL = (40000 - 50000) * 0.00000005 = -10000 * 0.00000005 = -0.0005
    # Net PnL = -0.0005 - 0.01 = -0.0105
    assert reduced.realized_pnl == pytest.approx(-0.0105)

# --- 3. Reconcile Full Tests ---

class _DummySession:
    def __init__(self):
        self.execute = AsyncMock()
        self.commit = AsyncMock()
        self.flush = AsyncMock()
        self.add = MagicMock()
        self.begin = MagicMock()
        self.begin.return_value.__aenter__ = AsyncMock()
        self.begin.return_value.__aexit__ = AsyncMock()

    def __call__(self): return self # allow session() calling
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass

@pytest.mark.asyncio
async def test_reconcile_full_restores_missing_sl_tp(monkeypatch):
    exchange = MagicMock()
    # Mocking exchange response: 1 active position with 1.0 BTC
    exchange.fetch_positions = AsyncMock(return_value=[
        {'symbol': 'BTC/USDT', 'contracts': 1.0, 'side': 'long', 'entryPrice': 50000.0}
    ])
    exchange.fetch_open_orders = AsyncMock(return_value=[])
    # No existing orders on exchange (orphaned position)
    exchange.request = AsyncMock(side_effect=[
        {'algoOrders': []}, # 1. reconcile_full: fetch_algo_orders (all)
        {'algoId': 'new-sl-id'}, # 2. _set_protective_orders (SL): create
        {'algoId': 'new-tp-id'}, # 3. _set_protective_orders (TP): create
        {'algoOrders': []}, # 4. _cancel_extra_algo_orders: fetch
        {'algoOrders': []}, # Extra buffer
    ])
    
    rm = RiskManager()
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=rm)
    engine._get_position_mode = AsyncMock(return_value=False)
    engine._normalize_price = AsyncMock(side_effect=lambda _s, p: p)
    engine._normalize_amount = AsyncMock(side_effect=lambda _s, a: a)
    
    # Mock DB: Position exists in DB but without SL/TP IDs
    db_pos = PositionModel(
        id=1, symbol='BTC/USDT', status=PositionStatus.OPEN, 
        size=1.0, entry_price=50000.0, stop_loss=49000.0, take_profit=55000.0
    )
    
    session = _DummySession()
    # Mock for SELECT in reconcile_full
    res_select = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [db_pos]))
    res_select.scalar_one_or_none = MagicMock(return_value=None)
    session.execute.return_value = res_select
    
    engine._get_live_position = AsyncMock(return_value=(1.0, "LONG"))
    engine._prepare_private_ops = AsyncMock()
    exchange.fetch_ticker = AsyncMock(return_value={"last": 50000.0})
    
    monkeypatch.setattr("core.execution.engine.async_session", session)
    
    await engine.reconcile_full()
    
    # Check if _set_protective_orders was called (via exchange.request)
    # Expected 4 calls: fetch-all (reconcile), SL-create, TP-create, cleanup-fetch
    assert exchange.request.await_count == 4
    assert engine.active_trades['BTC/USDT']['stop_order_id'] == 'new-sl-id'
    assert engine.active_trades['BTC/USDT']['tp_order_id'] == 'new-tp-id'

# --- 4. Integration Test: Virtual Position SL/TP Auto-creation ---

@pytest.mark.asyncio
async def test_integration_execute_signal_creates_sl_tp(monkeypatch):
    exchange = MagicMock()
    # 1. Entry order success
    exchange.create_order = AsyncMock(return_value={
        "id": "entry-123", "average": 100.0, "filled": 1.0, "status": "closed"
    })
    exchange.fetch_order = AsyncMock(return_value={
        "id": "entry-123", "average": 100.0, "filled": 1.0, "status": "closed"
    })
    exchange.fetch_positions = AsyncMock(return_value=[])
    exchange.fetch_open_orders = AsyncMock(return_value=[])
    exchange.fetch_ticker = AsyncMock(return_value={"last": 100.0})
    exchange.market = MagicMock(return_value={
        "limits": {"amount": {"min": 0.0001}, "cost": {"min": 0.1}}
    })
    # 2. SL/TP creation success (no pre-check anymore)
    exchange.request = AsyncMock(side_effect=[
        {"algoId": "sl-95"}, # SL
        {"algoId": "tp-110"}, # TP
    ])
    
    engine = ExecutionEngine(exchange_client=exchange, risk_manager=RiskManager())
    engine._get_position_mode = AsyncMock(return_value=False)
    engine._normalize_price = AsyncMock(side_effect=lambda _s, p: p)
    engine._normalize_amount = AsyncMock(side_effect=lambda _s, a: a)
    engine._set_leverage_best_effort = AsyncMock()
    engine._find_open_position_guard = AsyncMock(return_value=None)
    engine._get_live_position = AsyncMock(return_value=(0.0, None))
    engine._prepare_private_ops = AsyncMock()
    
    from config.settings import settings
    settings.is_trading_enabled = True
    settings.apply_new_entry_rules_after_flat = False # Simplify for test
    
    # Mock DB factory
    class MockSession(_DummySession):
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def __call__(self): return self # for async_session() call

    m_session = MockSession()
    # Mock for UPDATE in execute_signal
    res_update = SimpleNamespace(rowcount=1)
    res_update.scalar_one_or_none = MagicMock(return_value=None)
    m_session.execute.return_value = res_update

    monkeypatch.setattr("core.execution.engine.async_session", m_session)
    monkeypatch.setattr("core.execution.engine.send_telegram_msg", AsyncMock())
    
    # Signal
    signal = {
        "id": 1, "symbol": "BTC/USDT", "signal": "LONG", 
        "entry_price": 100.0, "atr": 2.5, "take_profit": 110.0
    }
    
    await engine.execute_signal(signal, account_balance=1000.0, drawdown=0.0, open_count=0)
    
    # Verify SL/TP orders were sent
    # 1 check + 1 SL + 1 TP
    # Verify SL/TP orders were sent
    # 1 check + 1 SL + 1 TP
    # In execute_signal, it calls _set_protective_orders which calls exchange.request
    assert exchange.request.await_count >= 2
    assert engine.active_trades["BTC/USDT"]["stop_order_id"] == "sl-95"
    assert engine.active_trades["BTC/USDT"]["tp_order_id"] == "tp-110"
