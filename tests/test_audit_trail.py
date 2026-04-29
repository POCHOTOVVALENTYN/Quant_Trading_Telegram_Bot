from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from core.audit.audit_trail import AuditTrail


class _DummySession:
    def __init__(self):
        self.add = MagicMock()
        self.commit = AsyncMock()


class _DummySessionCtx:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_audit_trail_persists_execution_event(monkeypatch):
    session = _DummySession()

    def _session_factory():
        return _DummySessionCtx(session)

    monkeypatch.setattr("core.audit.audit_trail.async_session", _session_factory)

    trail = AuditTrail(user_id=7)
    import asyncio
    asyncio.run(
        trail.record(
            event_type="position_opened",
            symbol="BTC/USDT",
            strategy="Donchian",
            signal_id=11,
            position_id=22,
            message="Position opened after entry fill",
            payload={"qty": 1.5, "entry_price": 100.0},
        )
    )

    session.add.assert_called_once()
    session.commit.assert_awaited_once()
