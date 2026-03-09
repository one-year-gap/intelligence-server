from collections.abc import AsyncGenerator

from pgvector.asyncpg import register_vector
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings

settings = get_settings()


def create_engine() -> AsyncEngine:
    database_url = settings.effective_database_url
    connect_args = {}
    if settings.database_ssl:
        connect_args["ssl"] = True  # RDS 환경에서 SSL
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )


# DATABASE_URL 없으면 엔진 미생성(앱 기동은 가능, DB 사용 시점에 에러)
engine: AsyncEngine | None = None
SessionLocal: async_sessionmaker[AsyncSession] | None = None

if (settings.effective_database_url or "").strip():
    engine = create_engine()
    # pgvector 등록: asyncpg 연결 시 vector 타입 등록
    @event.listens_for(engine.sync_engine, "connect")
    def register_pgvector(dbapi_connection, connection_record) -> None:
        dbapi_connection.run_async(register_vector)

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    if SessionLocal is None:
        raise RuntimeError(
            "Database is not configured. "
            "Set DATABASE_URL/DB_URL or POSTGRES_HOST/POSTGRES_PORT/POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD."
        )
    async with SessionLocal() as session:
        yield session


async def check_db_connection() -> None:
    if engine is None:
        return
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
