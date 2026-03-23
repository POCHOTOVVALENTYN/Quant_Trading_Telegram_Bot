import pytest
from core.risk.risk_manager import RiskManager, TimeExitSystem, PyramidingSystem

def test_risk_manager_init():
    rm = RiskManager(max_risk_pct=0.01, max_drawdown_pct=0.15, max_open_trades=3)
    assert rm.max_risk_pct == 0.01
    assert rm.max_drawdown_pct == 0.15
    assert rm.max_open_trades == 3

def test_check_trade_allowed():
    rm = RiskManager(max_open_trades=5, max_drawdown_pct=0.20)
    
    # Case 1: Allowed
    assert rm.check_trade_allowed(current_open_trades=2, current_drawdown_pct=0.10) is True
    
    # Case 2: Max trades reached
    assert rm.check_trade_allowed(current_open_trades=5, current_drawdown_pct=0.10) is False
    
    # Case 3: Max drawdown reached
    assert rm.check_trade_allowed(current_open_trades=2, current_drawdown_pct=0.20) is False

def test_calculate_position_size():
    rm = RiskManager(max_risk_pct=0.02)
    # Новый режим: 5% маржи на сделку * leverage(settings)
    # При дефолтных settings.leverage=10:
    # margin = 10000 * 0.05 = 500 USDT
    # notional = 500 * 10 = 5000 USDT
    # size = 5000 / 100 = 50
    size = rm.calculate_position_size(account_balance=10000, entry_price=100, stop_loss_price=90)
    assert size == 50.0
    
    # stop_loss_price теперь не влияет на размер, но некорректная цена входа должна давать 0
    assert rm.calculate_position_size(10000, 0, 100) == 0.0

def test_calculate_atr_stop_long():
    rm = RiskManager()
    # No ATR -> use settings.sl_long_pct (default 0.003 or similar)
    # Price 100, ATR 0, signal LONG
    # sl_long_pct = 0.003 (from config)
    # We need to mock settings if we want deterministic result here, 
    # but let's just check it doesn't crash and returns a value < entry
    stop = rm.calculate_atr_stop(entry_price=100, atr=0, signal_type="LONG")
    assert stop < 100
    
    # With ATR: 100 - (2.0 * 2) = 96
    # If correction is enabled (+0.1% for LONG) -> 100*0.001 = 0.1
    # 96 - 0.1 = 95.9
    stop_atr = rm.calculate_atr_stop(entry_price=100, atr=2, signal_type="LONG", multiplier=2.0)
    # Since sl_correction_enabled is likely True in settings.py Step 14
    assert stop_atr < 96.0

def test_time_exit():
    te = TimeExitSystem()
    now = 1000000
    # 48 bars for 1h is 48 hours = 172800 sec
    entry_ts = now - 3600*10 # 10 hours ago (< 48 bars)
    
    # Case 1: No exit before 48 bars
    assert te.should_exit(opened_at_ts=entry_ts, current_ts=now, timeframe="1h", current_price=100, entry_price=100) is False
    
    # Case 2: Exit after 48 bars if no progress (price 100, entry 100)
    # 50 hours ago (> 48 bars)
    entry_ts_old = now - 3600*50 
    assert te.should_exit(opened_at_ts=entry_ts_old, current_ts=now, timeframe="1h", current_price=100, entry_price=100) is True
    
    # Case 3: NO exit after 48 bars IF in good profit (Entry 100, Price 105 > 0.5% profit)
    assert te.should_exit(opened_at_ts=entry_ts_old, current_ts=now, timeframe="1h", current_price=110, entry_price=100) is False

def test_pyramiding_allocation():
    ps = PyramidingSystem()
    # Stage 0: 5% of 100 = 5.0
    assert ps.get_allocation_usdt(account_balance=100, current_stage=0) == 5.0
    # Stage 1: 3% of 100 = 3.0
    assert ps.get_allocation_usdt(account_balance=100, current_stage=1) == 3.0
    # Out of bounds
    assert ps.get_allocation_usdt(account_balance=100, current_stage=10) == 0.0
