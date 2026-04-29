from __future__ import annotations

import asyncio
from logging.config import fileConfig

import os
from dotenv import load_dotenv

# Явная загрузка .env для Alembic
load_dotenv(".env")

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from config.settings import settings
from database.session import Base
from database.models import all_models  # noqa: F401  (ensure models imported)


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _set_sqlalchemy_url():
    # Use app settings instead of placeholder in alembic.ini
    url = os.getenv("DATABASE_URL")
    if not url:
        url = settings.database_url
    
    # Убираем кавычки, если они есть
    if url.startswith('"') and url.endswith('"'):
        url = url[1:-1]
        
    print(f"DEBUG: Alembic using URL: {url}")
    config.set_main_option("sqlalchemy.url", url)


def run_migrations_offline() -> None:
    _set_sqlalchemy_url()
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection, 
        target_metadata=target_metadata, 
        compare_type=True,
        render_as_batch=True  # КРИТИЧНО для SQLite (позволяет изменять таблицы)
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    _set_sqlalchemy_url()
    
    # Для SQLite проверяем наличие файла и создаем базу через metadata если его нет
    url = config.get_main_option("sqlalchemy.url")
    if url.startswith("sqlite"):
        db_path = url.replace("sqlite+aiosqlite:///", "")
        if not os.path.exists(db_path):
            print(f"📦 SQLite: Initializing new database at {db_path}...")
            from sqlalchemy.ext.asyncio import create_async_engine
            temp_engine = create_async_engine(url)
            async with temp_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await temp_engine.dispose()
            
            # В SQLite при первой инициализации мы не можем использовать alembic stamp 
            # изнутри асинхронного цикла из-за рекурсии. 
            # Просто выходим — база уже создана со всеми актуальными таблицами.
            print("✅ Database tables created directly from models.")
            return

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online_sync() -> None:
    asyncio.run(run_migrations_online())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online_sync()
