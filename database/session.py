from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

from config.settings import settings

Base = declarative_base()

# Async Engine
engine_params = {
    "echo": False,
    "pool_pre_ping": True,
}

# SQLite не поддерживает pool_size и max_overflow
if not settings.database_url.startswith("sqlite"):
    engine_params.update({
        "pool_size": 10,
        "max_overflow": 20,
        "pool_recycle": 3600
    })

engine = create_async_engine(
    settings.database_url,
    **engine_params
)

# Shared async session factory
async_session = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

async def get_db_session() -> AsyncSession: # type: ignore
    async with async_session() as session:
        yield session
