"""Persist POST /api/v1/runtime-settings/* fields to Postgres for multi-instance convergence."""

from __future__ import annotations

from typing import Any, Iterable, Optional

from sqlalchemy import select

from config.settings import settings
from database.session import async_session
from utils.logger import app_logger

RUNTIME_PERSIST_KEYS: tuple[str, ...] = (
    "pyramiding_enabled",
    "per_trade_margin_pct",
    "position_size_usdt",
    "max_open_trades",
    "leverage",
    "tp_pct",
    "signal_expiry_seconds",
    "allowed_position_side",
)


def build_runtime_persist_payload() -> dict[str, Any]:
    return {k: getattr(settings, k) for k in RUNTIME_PERSIST_KEYS}


def apply_runtime_persist_payload(
    data: dict[str, Any],
    *,
    orchestrator: Any = None,
    keys: Optional[Iterable[str]] = None,
) -> None:
    if not data:
        return
    use_keys = tuple(keys) if keys is not None else RUNTIME_PERSIST_KEYS
    for k in use_keys:
        if k not in data:
            continue
        setattr(settings, k, data[k])
    if orchestrator is not None and getattr(orchestrator, "execution", None) and getattr(
        orchestrator.execution, "risk_manager", None
    ):
        orchestrator.execution.risk_manager.max_open_trades = int(settings.max_open_trades)


async def load_runtime_settings_from_database(*, orchestrator: Any = None) -> None:
    from database.models.all_models import RuntimeEngineSettings

    try:
        async with async_session() as session:
            result = await session.execute(select(RuntimeEngineSettings).where(RuntimeEngineSettings.id == 1))
            row = result.scalar_one_or_none()
            if not row or not row.payload:
                return
            apply_runtime_persist_payload(dict(row.payload), orchestrator=orchestrator)
            app_logger.info("Runtime engine settings restored from database.")
    except Exception as e:
        app_logger.warning(f"Runtime settings DB load skipped: {e}")


async def persist_runtime_settings_snapshot() -> None:
    from database.models.all_models import RuntimeEngineSettings

    snap = build_runtime_persist_payload()
    try:
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(select(RuntimeEngineSettings).where(RuntimeEngineSettings.id == 1))
                row = result.scalar_one_or_none()
                if row is None:
                    session.add(RuntimeEngineSettings(id=1, payload=snap))
                else:
                    row.payload = snap
    except Exception as e:
        app_logger.warning(f"Runtime settings DB persist failed: {e}")
