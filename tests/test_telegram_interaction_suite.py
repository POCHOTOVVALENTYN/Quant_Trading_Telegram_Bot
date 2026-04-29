import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from api.telegram import main as tg


class _Msg:
    def __init__(self, text=None):
        self.text = text
        self.sent = []

    async def reply_text(self, text, **kwargs):
        self.sent.append({"text": text, **kwargs})


class _Resp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("http error")


class _HttpStub:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        fn = self.routes.get(("GET", url))
        return fn(url, kwargs) if fn else _Resp({})

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        fn = self.routes.get(("POST", url))
        return fn(url, kwargs) if fn else _Resp({})


def _mk_update(user_id=1, text=""):
    msg = _Msg(text=text)
    return SimpleNamespace(message=msg, effective_user=SimpleNamespace(id=user_id)), msg


def test_start_access_control(monkeypatch):
    monkeypatch.setattr(tg.settings, "admin_user_ids", "1")
    upd, msg = _mk_update(user_id=999)
    asyncio.run(tg.start(upd, None))
    assert "Доступ запрещен" in msg.sent[0]["text"]


def test_start_admin_renders_main_menu(monkeypatch):
    monkeypatch.setattr(tg.settings, "admin_user_ids", "1")
    upd, _ = _mk_update(user_id=1)
    render_mock = AsyncMock()
    monkeypatch.setattr(tg, "_render_main_menu", render_mock)
    asyncio.run(tg.start(upd, None))
    render_mock.assert_awaited_once()


def test_show_active_positions_empty(monkeypatch):
    upd, msg = _mk_update()
    url = f"{tg.ENGINE_URL}/api/v1/trades"
    stub = _HttpStub({("GET", url): lambda *_: _Resp({"trades": {}})})
    monkeypatch.setattr(tg.httpx, "AsyncClient", lambda *a, **k: stub)
    asyncio.run(tg.show_active_positions(upd, None))
    assert "Активных позиций" in msg.sent[0]["text"]


def test_show_active_positions_sends_summary_and_list(monkeypatch):
    upd, msg = _mk_update()
    url = f"{tg.ENGINE_URL}/api/v1/trades"
    trades = {
        "BTC/USDT": {
            "signal_type": "LONG",
            "entry": 100.0,
            "stop": 95.0,
            "current_price": 98.0,
            "pnl_usd": -2.0,
            "pnl_pct": -2.0,
            "current_size": 1.0,
            "opened_at": 0.0,
        }
    }
    stub = _HttpStub({("GET", url): lambda *_: _Resp({"trades": trades})})
    monkeypatch.setattr(tg.httpx, "AsyncClient", lambda *a, **k: stub)
    asyncio.run(tg.show_active_positions(upd, None))
    assert len(msg.sent) == 2
    assert "ОТКРЫТЫЕ ПОЗИЦИИ" in msg.sent[0]["text"]
    assert "СПИСОК ПОЗИЦИЙ" in msg.sent[1]["text"]


def test_show_trade_history_formats_reason(monkeypatch):
    upd, msg = _mk_update()
    url = f"{tg.ENGINE_URL}/api/v1/history"
    payload = {
        "items": [
            {"symbol": "BTC/USDT", "pnl_usd": 1.2, "pnl_pct": 0.5, "reason": "STOP", "closed_at": "2026-01-01T10:00:00"}
        ]
    }
    stub = _HttpStub({("GET", url): lambda *_: _Resp(payload)})
    monkeypatch.setattr(tg.httpx, "AsyncClient", lambda *a, **k: stub)
    asyncio.run(tg.show_trade_history(upd, None))
    assert len(msg.sent) == 1
    assert "ИСТОРИЯ СДЕЛОК" in msg.sent[0]["text"]
    assert "🛑 стоп" in msg.sent[0]["text"]


def test_handle_text_routes_to_active_positions(monkeypatch):
    monkeypatch.setattr(tg.settings, "admin_user_ids", "1")
    upd, _ = _mk_update(user_id=1, text=tg.BTN_ACTIVE)
    mocked = AsyncMock()
    monkeypatch.setattr(tg, "show_active_positions", mocked)
    asyncio.run(tg.handle_text(upd, None))
    mocked.assert_awaited_once()


def test_stress_cooldown_cleanup_large_map():
    tg._action_cooldowns.clear()
    tg._action_spam_score.clear()
    tg._action_spam_last_ts.clear()

    # Имитация большого количества разных ключей anti-spam.
    for i in range(3000):
        tg._action_cooldowns[f"k{i}"] = 0.0
    tg._cleanup_cooldown_map(now_ts=10_000.0)
    assert len(tg._action_cooldowns) <= tg._ACTION_COOLDOWN_MAX_KEYS


def test_stress_callback_burst_refresh_with_level_messages(monkeypatch):
    class _Q:
        def __init__(self):
            self.data = "pos_refresh_BTC_USDT"
            self.answers = []
            self.edits = []

        async def answer(self, text=None, show_alert=False):
            self.answers.append(text or "")

        async def edit_message_text(self, text, **kwargs):
            self.edits.append(text)

    trades_url = f"{tg.ENGINE_URL}/api/v1/trades"
    trades = {
        "BTC/USDT": {
            "signal_type": "LONG",
            "entry": 100.0,
            "stop": 95.0,
            "current_price": 99.0,
            "pnl_usd": -1.0,
            "pnl_pct": -1.0,
            "current_size": 1.0,
            "opened_at": 0.0,
        }
    }
    stub = _HttpStub({("GET", trades_url): lambda *_: _Resp({"trades": trades})})
    monkeypatch.setattr(tg.httpx, "AsyncClient", lambda *a, **k: stub)

    tg._action_cooldowns.clear()
    tg._action_spam_score.clear()
    tg._action_spam_last_ts.clear()

    monkeypatch.setattr(tg.settings, "admin_user_ids", "42")

    q = _Q()
    upd = SimpleNamespace(callback_query=q, effective_user=SimpleNamespace(id=42))
    for _ in range(12):
        asyncio.run(tg.callback_handler(upd, None))

    # Часть запросов должна попасть под cooldown и вернуть lvl-индикатор.
    assert any("lvl " in a for a in q.answers)
    assert len(q.edits) >= 1

