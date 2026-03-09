import asyncpg
from asyncpg import Pool

from app.core.config import Settings


async def create_postgres_pool(settings: Settings) -> Pool:
    dsn = settings.effective_postgres_dsn
    if not dsn:
        raise RuntimeError(
            "PostgreSQL is not configured. Set POSTGRES_DSN or "
            "POSTGRES_HOST/POSTGRES_PORT/POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD."
        )
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=settings.postgres_pool_min_size,
        max_size=settings.postgres_pool_max_size,
    )
