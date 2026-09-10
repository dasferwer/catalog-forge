from sqlalchemy.ext.asyncio import create_async_engine

from .config import settings

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    connect_args={"server_settings": {"statement_timeout": "120000", "lock_timeout": "30000"}},
)
