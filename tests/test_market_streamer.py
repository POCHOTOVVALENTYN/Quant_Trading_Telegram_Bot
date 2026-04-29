import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.market_data.market_streamer import MarketDataService


def _exchange():
    ex = MagicMock()
    ex.urls = {
        "api": {"ws": {"future": "wss://example/ws"}},
        "test": {"ws": {"future": "wss://example-test/ws"}},
    }
    ex.clients = {}
    return ex


def test_watchdog_threshold_scales_with_timeframe():
    svc = MarketDataService(["BTC/USDT"], ["1m", "15m", "1h"], exchange=_exchange())
    assert svc._soft_stale_threshold_seconds("1m") == 120.0
    assert svc._soft_stale_threshold_seconds("15m") == 225.0
    assert svc._hard_stale_threshold_seconds("15m") == 450.0
    assert svc._hard_stale_threshold_seconds("1h") == 1800.0


@pytest.mark.asyncio
async def test_watchdog_marks_stream_recovering_without_refreshing_timestamp(monkeypatch):
    svc = MarketDataService(["BTC/USDT"], ["1m"], exchange=_exchange())
    svc.running = True
    svc.last_candle_time["BTC/USDT:1m"] = 0.0
    svc._close_ws_clients = AsyncMock()

    loop = SimpleNamespace(time=lambda: 121.0)
    monkeypatch.setattr("services.market_data.market_streamer.asyncio.get_event_loop", lambda: loop)

    async def _fake_sleep(_sec):
        svc.running = False

    monkeypatch.setattr("services.market_data.market_streamer.asyncio.sleep", _fake_sleep)
    await svc._watchdog_loop()

    assert svc.last_candle_time["BTC/USDT:1m"] == 0.0
    assert svc._stream_recovering["BTC/USDT:1m"] is True
    svc._close_ws_clients.assert_not_awaited()


@pytest.mark.asyncio
async def test_watchdog_does_not_trigger_for_15m_gap_under_scaled_threshold(monkeypatch):
    svc = MarketDataService(["BTC/USDT"], ["15m"], exchange=_exchange())
    svc.running = True
    svc.last_candle_time["BTC/USDT:15m"] = 0.0
    svc._close_ws_clients = AsyncMock()

    loop = SimpleNamespace(time=lambda: 200.0)  # 200s < 225s soft threshold
    monkeypatch.setattr("services.market_data.market_streamer.asyncio.get_event_loop", lambda: loop)

    async def _fake_sleep(_sec):
        svc.running = False

    monkeypatch.setattr("services.market_data.market_streamer.asyncio.sleep", _fake_sleep)
    await svc._watchdog_loop()

    assert svc._stream_recovering.get("BTC/USDT:15m") is False
    svc._close_ws_clients.assert_not_awaited()
