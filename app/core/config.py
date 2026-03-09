from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "counseling-analytics"
    app_env: str = "local"
    debug: bool = False

    # 실제 운영 => EFS 마운트 경로를 지정
    efs_base_dir: Path = Path("./data/efs")
    state_dir: Path = Path("./data/state")

    # Kafka consumer
    kafka_consumer_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_analysis_request_topic: str = "analysis.request.v1"
    kafka_consumer_group_id: str = "counseling-analytics-consumer"
    kafka_auto_offset_reset: str = "earliest"
    kafka_batch_size: int = 1000
    kafka_poll_timeout_ms: int = 1000
    kafka_log_each_message: bool = False
    kafka_log_result_limit: int = 20

    # PostgreSQL
    postgres_dsn: str = "postgresql://postgres:postgres@127.0.0.1:5432/holliverse"
    postgres_pool_min_size: int = 1
    postgres_pool_max_size: int = 10

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
