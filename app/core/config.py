from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import os


class Settings(BaseSettings):
    app_name: str = "counseling-analytics"
    app_env: str = "local"
    debug: bool = False

    api_v1_prefix: str = "/api/v1"
    log_level: str = "INFO"

    # recommendation (미설정 시 빈 문자열, DB/OpenAI 사용 시점에 연결 실패)
    database_url: str = Field(default="", validation_alias=AliasChoices("DATABASE_URL", "DB_URL"))
    database_ssl: bool = Field(default=False, validation_alias=AliasChoices("DATABASE_SSL", "DB_SSL"))
    openai_api_key: str = ""
    openai_chat_model: str = "gpt-4o-mini"
    openai_embedding_model: str = "text-embedding-3-small"
    recommend_top_k: int = 3
    cache_ttl_days: int = 7

    # 실제 운영 => EFS 마운트 경로를 지정
    efs_base_dir: Path = Path("./data/efs")
    state_dir: Path = Path("./data/state")

    # Kafka consumer (analysis request)
    kafka_consumer_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_analysis_request_topic: str = "analysis.request.v1"
    kafka_consumer_group_id: str = "counseling-analytics-consumer"
    kafka_auto_offset_reset: str = "earliest"
    kafka_batch_size: int = 1000
    kafka_poll_timeout_ms: int = 1000
    kafka_log_each_message: bool = False
    kafka_log_result_limit: int = 20

    # PostgreSQL connection for bulk lookup
    postgres_dsn: str = Field(default="", validation_alias=AliasChoices("POSTGRES_DSN", "DB_DSN"))
    postgres_host: str = Field(default="", validation_alias=AliasChoices("POSTGRES_HOST", "DB_HOST"))
    postgres_port: int = Field(default=5432, validation_alias=AliasChoices("POSTGRES_PORT", "DB_PORT"))
    postgres_db: str = Field(
        default="",
        validation_alias=AliasChoices("POSTGRES_DB", "DB_NAME", "DB_DATABASE", "RDS_DB_NAME", "DBNAME"),
    )
    postgres_user: str = Field(
        default="",
        validation_alias=AliasChoices("POSTGRES_USER", "DB_USER", "RDS_USERNAME", "DB_USERNAME"),
    )
    postgres_password: str = Field(
        default="",
        validation_alias=AliasChoices("POSTGRES_PASSWORD", "DB_PASSWORD", "RDS_PASSWORD"),
    )
    postgres_sslmode: str = Field(default="", validation_alias=AliasChoices("POSTGRES_SSLMODE", "DB_SSLMODE"))
    postgres_pool_min_size: int = 1
    postgres_pool_max_size: int = 10

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def effective_postgres_dsn(self) -> str:
        explicit_dsn = self.postgres_dsn.strip()
        if explicit_dsn:
            return explicit_dsn

        if not (self.postgres_host and self.postgres_db and self.postgres_user and self.postgres_password):
            return ""

        user = quote(self.postgres_user, safe="")
        password = quote(self.postgres_password, safe="")
        dsn = f"postgresql://{user}:{password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

        sslmode = self.postgres_sslmode.strip()
        if sslmode:
            dsn = f"{dsn}?sslmode={sslmode}"

        return dsn

    @property
    def effective_database_url(self) -> str:
        explicit_url = self.database_url.strip()
        if explicit_url:
            return explicit_url

        dsn = self.effective_postgres_dsn
        if not dsn:
            return ""
        if dsn.startswith("postgresql+asyncpg://"):
            return dsn
        if dsn.startswith("postgresql://"):
            return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
        if dsn.startswith("postgres://"):
            return dsn.replace("postgres://", "postgresql+asyncpg://", 1)
        return dsn


@lru_cache
def get_settings() -> Settings:
    app_env = os.getenv("APP_ENV", "local")
    candidate = Path(f".env.{app_env}")
    env_file = str(candidate) if candidate.exists() else ".env"

    return Settings(_env_file=env_file)
