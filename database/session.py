from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

from config.settings import settings

Base = declarative_base()

# Async Engine
engine = create_async_engine(
    settings.database_url, 
    echo=False,
    pool_size=10, 
    max_overflow=20,
    pool_pre_ping=True,       # Проверка живого соединения перед использованием
    pool_recycle=3600         # Сброс старых соединений раз в час
)

# Shared async session factory
async_session = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

async def get_db_session() -> AsyncSession: # type: ignore
    async with async_session() as session:
        yield session
