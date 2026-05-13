"""Runtime settings API contract and DB snapshot persistence."""

import pytest

pytest.importorskip("aiosqlite")
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.schema_version import CONFIG_SCHEMA_VERSION


@pytest.mark.asyncio
async def test_get_runtime_settings_includes_config_schema_version(monkeypatch):
    pytest.importorskip("fastapi")
    from api.rest import main as api_main

    out = await api_main.get_runtime_settings()
    assert out["config_schema_version"] == CONFIG_SCHEMA_VERSION
    assert "pyramiding_enabled" in out


@pytest.mark.asyncio
async def test_runtime_settings_store_sqlite_roundtrip(monkeypatch):
    from database.models.all_models import RuntimeEngineSettings
    from config.settings import settings
    from services import runtime_settings_store as rss

    eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with eng.begin() as conn:
        await conn.run_sync(RuntimeEngineSettings.__table__.create, checkfirst=True)
    factory = async_sessionmaker(eng, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(rss, "async_session", factory)

    prev = settings.pyramiding_enabled
    try:
        settings.pyramiding_enabled = True
        await rss.persist_runtime_settings_snapshot()
        settings.pyramiding_enabled = False
        await rss.load_runtime_settings_from_database(orchestrator=None)
        assert settings.pyramiding_enabled is True
    finally:
        settings.pyramiding_enabled = prev
    await eng.dispose()
