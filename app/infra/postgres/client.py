import asyncpg
from asyncpg import Pool

from app.core.config import Settings


async def create_postgres_pool(settings: Settings) -> Pool:
    return await asyncpg.create_pool(
        dsn=settings.postgres_dsn,
        min_size=settings.postgres_pool_min_size,
        max_size=settings.postgres_pool_max_size,
    )
