from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import settings

auth_engine = create_async_engine(settings.auth_db_url, echo=False, pool_size=3, max_overflow=5)
auth_async_session = async_sessionmaker(auth_engine, class_=AsyncSession, expire_on_commit=False)


async def get_auth_db():
    async with auth_async_session() as session:
        try:
            yield session
        finally:
            await session.close()
