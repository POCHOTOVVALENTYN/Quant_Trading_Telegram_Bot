import asyncio
from types import SimpleNamespace

from api.telegram import main as tg


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _FakeAsyncClient:
    def __init__(self, *, trades_payload, calls, reduce_payload=None):
        self._trades_payload = trades_payload
        self.calls = calls
        self._reduce_payload = reduce_payload or {"status": "success"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        if url.endswith("/api/v1/trades"):
            return _FakeResponse({"trades": self._trades_payload})
        return _FakeResponse({})

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if "/api/v1/trades/reduce/" in url:
            return _FakeResponse(self._reduce_payload)
        if "/api/v1/trades/close/" in url:
            return _FakeResponse({"status": "success"})
        return _FakeResponse({"status": "error", "message": "unsupported"})


class _FakeQuery:
    def __init__(self, data):
        self.data = data
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append({"text": text, "show_alert": show_alert})

    async def edit_message_text(self, text, **kwargs):
        self.edits.append({"text": text, **kwargs})


def _trades_payload():
    return {
        "BTC/USDT": {
            "signal_type": "LONG",
            "entry": 100.0,
            "stop": 95.0,
            "current_price": 96.0,
            "pnl_usd": -4.0,
            "pnl_pct": -4.0,
            "current_size": 1.0,
            "opened_at": 0.0,
        },
        "ETH/USDT": {
            "signal_type": "LONG",
            "entry": 100.0,
            "stop": 95.0,
            "current_price": 102.0,
            "pnl_usd": 2.0,
            "pnl_pct": 2.0,
            "current_size": 1.0,
            "opened_at": 0.0,
        },
    }


def _run_handler(
    monkeypatch,
    data,
    calls,
    *,
    trades_payload=None,
    reduce_payload=None,
    reset_cooldowns=True,
):
    if reset_cooldowns:
        tg._action_cooldowns.clear()
        tg._action_spam_score.clear()
        tg._action_spam_last_ts.clear()

    monkeypatch.setattr(tg.settings, "admin_user_ids", "777")

    fake_query = _FakeQuery(data)
    update = SimpleNamespace(
        callback_query=fake_query,
        effective_user=SimpleNamespace(id=777),
    )
    monkeypatch.setattr(
        tg.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _FakeAsyncClient(
            trades_payload=_trades_payload() if trades_payload is None else trades_payload,
            calls=calls,
            reduce_payload=reduce_payload,
        ),
    )
    asyncio.run(tg.callback_handler(update, context=None))
    return fake_query


def test_callback_handler_pos_nav_renders_vip_card(monkeypatch):
    calls = []
    q = _run_handler(monkeypatch, "pos_nav_ETH_USDT", calls)

    assert any(c[0] == "GET" and c[1].endswith("/api/v1/trades") for c in calls)
    assert len(q.edits) == 1
    assert "ETH/USDT" in q.edits[0]["text"]
    assert "🧭" in q.edits[0]["text"]
    kb = q.edits[0]["reply_markup"].inline_keyboard
    assert kb[1][0].callback_data.startswith("pos_refresh_")
    assert kb[1][1].callback_data.startswith("pos_reduce25_")
    assert kb[1][2].callback_data.startswith("pos_reduce50_")


def test_callback_handler_pos_refresh_updates_current_card(monkeypatch):
    calls = []
    q = _run_handler(monkeypatch, "pos_refresh_BTC_USDT", calls)

    assert any(c[0] == "GET" and c[1].endswith("/api/v1/trades") for c in calls)
    assert not any(c[0] == "POST" and "/trades/reduce/" in c[1] for c in calls)
    assert len(q.edits) == 1
    assert "BTC/USDT" in q.edits[0]["text"]


def test_callback_handler_pos_reduce25_and_50_call_reduce_endpoint(monkeypatch):
    for cb_data, expected_fraction in (("pos_reduce25_BTC_USDT", 0.25), ("pos_reduce50_BTC_USDT", 0.5)):
        calls = []
        q = _run_handler(monkeypatch, cb_data, calls)

        reduce_calls = [c for c in calls if c[0] == "POST" and "/api/v1/trades/reduce/" in c[1]]
        assert len(reduce_calls) == 1
        assert reduce_calls[0][2]["params"]["fraction"] == expected_fraction
        assert any("Сокращение" in (a.get("text") or "") and "✅" in (a.get("text") or "") for a in q.answers)
        assert len(q.edits) == 1


def test_callback_handler_close_calls_close_endpoint(monkeypatch):
    calls = []
    q = _run_handler(monkeypatch, "close_BTC_USDT", calls)

    close_calls = [c for c in calls if c[0] == "POST" and "/api/v1/trades/close/" in c[1]]
    assert len(close_calls) == 1
    assert close_calls[0][1].endswith("/api/v1/trades/close/BTC_USDT")
    assert len(q.edits) == 1
    assert "успешно закрыта вручную" in q.edits[0]["text"]


def test_callback_handler_reduce_error_shows_alert_and_keeps_card(monkeypatch):
    calls = []
    q = _run_handler(
        monkeypatch,
        "pos_reduce25_BTC_USDT",
        calls,
        reduce_payload={"status": "error", "message": "exchange timeout"},
    )

    reduce_calls = [c for c in calls if c[0] == "POST" and "/api/v1/trades/reduce/" in c[1]]
    assert len(reduce_calls) == 1
    assert any("Сокращение не выполнено" in (a.get("text") or "") and a.get("show_alert") for a in q.answers)
    assert len(q.edits) == 1
    assert "BTC/USDT" in q.edits[0]["text"]


def test_callback_handler_pos_nav_with_empty_positions(monkeypatch):
    calls = []
    q = _run_handler(monkeypatch, "pos_nav_BTC_USDT", calls, trades_payload={})

    assert len(q.edits) == 1
    assert "Активных позиций на данный момент нет" in q.edits[0]["text"]


def test_callback_handler_position_closed_between_clicks_returns_list(monkeypatch):
    calls = []
    # В payload есть только BTC, а кликаем по ETH
    one_trade = {"BTC/USDT": _trades_payload()["BTC/USDT"]}
    q = _run_handler(monkeypatch, "pos_nav_ETH_USDT", calls, trades_payload=one_trade)

    assert len(q.edits) == 1
    assert "Позиция `ETH/USDT` уже закрыта" in q.edits[0]["text"]
    assert "СПИСОК ПОЗИЦИЙ" in q.edits[0]["text"]


def test_callback_handler_cooldown_block_contains_lvl(monkeypatch):
    tg._action_cooldowns.clear()
    tg._action_spam_score.clear()
    tg._action_spam_last_ts.clear()

    calls_first = []
    q1 = _run_handler(monkeypatch, "pos_reduce25_BTC_USDT", calls_first, reset_cooldowns=False)
    assert any("Сокращение" in (a.get("text") or "") and "✅" in (a.get("text") or "") for a in q1.answers)

    calls_second = []
    q2 = _run_handler(monkeypatch, "pos_reduce25_BTC_USDT", calls_second, reset_cooldowns=False)
    assert any("lvl " in (a.get("text") or "") for a in q2.answers)
    assert any("Слишком часто" in (a.get("text") or "") for a in q2.answers)

