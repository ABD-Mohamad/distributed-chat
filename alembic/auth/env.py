import asyncio
import os
from logging.config import fileConfig

from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = os.getenv(
    "AUTH_DATABASE_URL",
    "postgresql+asyncpg://nexuschat:nexuschat@localhost:5432/nexuschat",
)


def run_migrations_online():
    def do_migrations(connection):
        context.configure(connection=connection, target_metadata=None)
        with context.begin_transaction():
            context.run_migrations()

    async def run_async_migrations():
        connectable = create_async_engine(database_url)
        async with connectable.connect() as connection:
            await connection.run_sync(do_migrations)
        await connectable.dispose()

    asyncio.run(run_async_migrations())


run_migrations_online()
