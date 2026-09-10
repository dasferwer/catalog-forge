import asyncio
import os

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

config = context.config
url = os.environ.get(
    "MIGRATION_DATABASE_URL",
    os.environ.get("DATABASE_URL", config.get_main_option("sqlalchemy.url")),
)


def run(connection):
    context.configure(connection=connection, target_metadata=None)
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = create_async_engine(url, poolclass=NullPool)
    async with engine.connect() as conn:
        await conn.run_sync(run)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=url, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(online())
