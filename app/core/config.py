from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "counseling-analytics"
    app_env: str = "local"
    debug: bool = False

    api_v1_prefix: str = "/api/v1"

    # 실제 운영 => EFS 마운트 경로를 지정
    efs_base_dir: Path = Path("./data/efs")
    state_dir: Path = Path("./data/state")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
