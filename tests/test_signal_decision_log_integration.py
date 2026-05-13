"""
Интеграционные проверки журнала решений по сигналам (SQLite in-memory).

Требуется пакет aiosqlite (см. requirements.txt). Без него тесты пропускаются.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.mark.asyncio
async def test_persist_decision_log_inserts_signal_decision_log_row(monkeypatch):
    pytest.importorskip("aiosqlite")

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from database.models.all_models import SignalDecisionLog
    from database.session import Base
    from services.signal_engine.engine import TradingOrchestrator

    eng = create_async_engine("sqlite+aiosqlite:///:memory:")

    async with eng.begin() as conn:

        def _create_tables(sync_conn):
            Base.metadata.create_all(sync_conn, tables=[SignalDecisionLog.__table__])

        await conn.run_sync(_create_tables)

    factory = async_sessionmaker(eng, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr("services.signal_engine.engine.async_session", factory)

    dummy = object.__new__(TradingOrchestrator)
    sdl = {
        "symbol": "BTC/USDT",
        "timeframe": "15m",
        "strategy": "Donchian",
        "direction": "LONG",
        "entry_price": 100.0,
        "adx": 25.0,
        "atr": 1.0,
        "rsi": 50.0,
        "volume_ratio": 1.0,
        "funding_rate": 0.0,
        "regime": "TREND",
        "daily_bias": None,
        "volatility_regime": "NORMAL",
        "funding_regime": "NORMAL",
        "session": "US",
        "score": 0.62,
        "win_prob": None,
        "ai_recommendation": None,
        "ai_confidence": None,
    }
    flags = {"f_weekly_filter": True, "f_cvd": False}
    await TradingOrchestrator._persist_decision_log(dummy, sdl, flags, "FILTERED:cvd")

    async with factory() as s:
        rows = (await s.execute(select(SignalDecisionLog))).scalars().all()

    await eng.dispose()

    assert len(rows) == 1
    row = rows[0]
    assert row.outcome == "FILTERED:cvd"
    assert row.symbol == "BTC/USDT"
    assert row.strategy == "Donchian"
    assert row.f_cvd is False
    assert row.f_weekly_filter is True
    assert row.score == pytest.approx(0.62)


@pytest.mark.asyncio
async def test_persist_decision_log_accepted_row(monkeypatch):
    pytest.importorskip("aiosqlite")

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from database.models.all_models import SignalDecisionLog
    from database.session import Base
    from services.signal_engine.engine import TradingOrchestrator

    eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with eng.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn, tables=[SignalDecisionLog.__table__]
            )
        )

    factory = async_sessionmaker(eng, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr("services.signal_engine.engine.async_session", factory)

    dummy = object.__new__(TradingOrchestrator)
    sdl = {
        "symbol": "ETH/USDT",
        "timeframe": "1h",
        "strategy": "MA Trend",
        "direction": "SHORT",
        "entry_price": 2000.0,
        "adx": 30.0,
        "atr": 10.0,
        "rsi": 55.0,
        "volume_ratio": 1.1,
        "funding_rate": 0.0001,
        "regime": "TREND",
        "daily_bias": "SHORT",
        "volatility_regime": "NORMAL",
        "funding_regime": "NORMAL",
        "session": "EU",
        "score": 0.7,
        "win_prob": 0.6,
        "ai_recommendation": "ENTER",
        "ai_confidence": 0.8,
    }
    await TradingOrchestrator._persist_decision_log(dummy, sdl, {}, "ACCEPTED")

    async with factory() as s:
        rows = (await s.execute(select(SignalDecisionLog))).scalars().all()
    await eng.dispose()

    assert len(rows) == 1
    assert rows[0].outcome == "ACCEPTED"
    assert rows[0].direction == "SHORT"
    assert rows[0].win_prob == pytest.approx(0.6)
