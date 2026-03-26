import importlib.util
from pathlib import Path

import pytest


_TG_MAIN_PATH = Path(__file__).resolve().parents[1] / "api" / "telegram" / "main.py"
_TG_SPEC = importlib.util.spec_from_file_location("telegram_main_for_tests", _TG_MAIN_PATH)
tg = importlib.util.module_from_spec(_TG_SPEC)
assert _TG_SPEC and _TG_SPEC.loader
_TG_SPEC.loader.exec_module(tg)


def _sample_trades():
    return {
        "BTC/USDT": {
            "signal_type": "LONG",
            "entry": 100.0,
            "stop": 95.0,
            "current_price": 96.0,  # ближе к стопу => высокий риск
            "pnl_usd": -4.0,
            "pnl_pct": -4.0,
            "current_size": 1.0,
            "opened_at": 0.0,
        },
        "ETH/USDT": {
            "signal_type": "LONG",
            "entry": 100.0,
            "stop": 95.0,
            "current_price": 102.0,  # дальше от стопа => низкий риск
            "pnl_usd": 2.0,
            "pnl_pct": 2.0,
            "current_size": 1.0,
            "opened_at": 0.0,
        },
    }


def test_sorted_positions_by_risk_prioritizes_high_risk_first():
    trades = _sample_trades()
    sorted_items = tg._sorted_positions_by_risk(trades)

    assert sorted_items[0][1] == "BTC/USDT"
    assert sorted_items[0][3] == "🔴 Высокий"
    assert sorted_items[1][1] == "ETH/USDT"


def test_build_positions_list_view_contains_callbacks_and_order():
    text, markup = tg._build_positions_list_view(_sample_trades())

    assert "СПИСОК ПОЗИЦИЙ" in text
    first_line = text.splitlines()[1]
    assert "BTC/USDT" in first_line

    rows = markup.inline_keyboard
    assert rows[0][0].callback_data == "pos_view_BTC_USDT"
    assert rows[0][1].callback_data == "close_BTC_USDT"


def test_build_position_details_view_contains_risk_and_pnl(monkeypatch):
    monkeypatch.setattr("api.telegram.main.time.time", lambda: 3600.0)
    info = {
        "signal_type": "LONG",
        "entry": 100.0,
        "stop": 95.0,
        "current_price": 96.0,
        "pnl_usd": -4.0,
        "pnl_pct": -4.0,
        "current_size": 1.5,
        "opened_at": 0.0,
    }

    text = tg._build_position_details_view("BTC/USDT", info)

    assert "🔹 **BTC/USDT**" in text
    assert "🚨 Риск до стопа: 🔴 Высокий" in text
    assert "📉 PnL: 🔴" in text


def test_adaptive_cooldown_and_spam_level(monkeypatch):
    # Сбрасываем in-memory карты перед тестом.
    tg._action_cooldowns.clear()
    tg._action_spam_score.clear()
    tg._action_spam_last_ts.clear()

    now = {"v": 1000.0}
    monkeypatch.setattr("api.telegram.main.time.time", lambda: now["v"])

    # Первый клик проходит.
    assert tg._is_action_on_cooldown(1, "close", "BTC_USDT") is False
    # Повтор сразу блокируется и поднимает score.
    assert tg._is_action_on_cooldown(1, "close", "BTC_USDT") is True
    assert tg._spam_level_badge(1, "close", "BTC_USDT").startswith("lvl ")

    # После паузы cooldown проходит.
    now["v"] += 3.0
    assert tg._is_action_on_cooldown(1, "close", "BTC_USDT") is False

