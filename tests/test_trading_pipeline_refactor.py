"""
Regression tests for trading pipeline refactor (settings, models, metrics, market worker).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_settings_pipeline_defaults():
    from config.settings import Settings

    s = Settings(telegram_bot_token=SecretStr("dummy_token_for_tests_only_12345"))
    assert s.error_window_seconds == 600.0
    assert s.error_threshold == 10
    assert s.use_post_only is False
    assert s.market_streamer_force_prod_chart_ws is True


def test_signal_model_has_failure_reason():
    text = (ROOT / "database/models/all_models.py").read_text()
    assert "failure_reason = Column" in text


def test_signal_decision_log_has_weekly_and_cvd_columns():
    text = (ROOT / "database/models/all_models.py").read_text()
    assert "f_weekly_filter = Column" in text
    assert "f_cvd = Column" in text


def test_metrics_exports_signal_decision_log_counter():
    from prometheus_client import generate_latest

    from utils.metrics import signal_decision_log_entries

    signal_decision_log_entries.labels(outcome_class="unit_test").inc()
    blob = generate_latest().decode()
    assert "trading_signal_decision_log_entries_total" in blob


@pytest.mark.asyncio
async def test_market_worker_payload_includes_wall_clock_fields():
    from services.market_data.worker import MarketDataWorker

    w = MarketDataWorker()
    w.redis = MagicMock()
    w.redis.publish = AsyncMock(return_value=1)

    await w._publish_to_redis("ohlcv", "BTC/USDT", "1m", [1, 2, 3, 4, 5, 6])

    w.redis.publish.assert_awaited_once()
    call_kw = w.redis.publish.await_args
    channel, raw = call_kw[0][0], call_kw[0][1]
    assert channel == "market:data"
    payload = json.loads(raw)
    assert "wall_ts" in payload
    assert "wall_ts_iso" in payload
    assert payload["type"] == "ohlcv"


def test_migration_file_exists_and_revisions():
    p = ROOT / "migrations/versions/20260511_signal_pipeline_observability.py"
    assert p.is_file()
    text = p.read_text()
    assert 'revision = "20260511_signal_pipeline_observability"' in text
    assert 'down_revision = "20260420_execution_audit_trail"' in text


def test_signal_loop_daily_halt_then_continue():
    eng = (ROOT / "services/signal_engine/engine.py").read_text()
    assert (
        'await self._persist_decision_log(sdl, _filter_flags, "FILTERED:daily_halt")\n'
        "                        continue"
    ) in eng


def test_signal_loop_max_positions_then_continue():
    eng = (ROOT / "services/signal_engine/engine.py").read_text()
    assert (
        'await self._persist_decision_log(sdl, _filter_flags, "FILTERED:max_positions")\n'
        "                        continue"
    ) in eng


def test_signal_loop_hunt_skips_with_decision_log():
    eng = (ROOT / "services/signal_engine/engine.py").read_text()
    assert "FILTERED:no_pending_setup" in eng
    assert "filtered_no_pending_setup" in eng


def test_priority_superseded_audit_trail():
    orch = (ROOT / "services/signal_engine/engine.py").read_text()
    exe = (ROOT / "core/execution/engine.py").read_text()
    assert "peer_priority_losers" in orch
    assert "peer_snapshots" in orch
    assert "async def _apply_priority_peer_supersede" in exe
    assert "FILTERED:priority_superseded" in exe
    assert "SUPERSEDED" in exe
    assert "filtered_priority_superseded" in exe
