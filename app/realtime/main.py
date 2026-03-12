"""FastAPI application entrypoint for realtime recommendation API."""

from __future__ import annotations

import logging
import os
import re

from fastapi import FastAPI

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.realtime.api.router import api_router

settings = get_settings()
configure_logging(settings.debug)


def _mask_database_url(url: str) -> str:
    """비밀번호만 마스킹한 URL (연결 대상 확인용)."""
    if not url:
        return "(empty)"
    return re.sub(r"(:[^:@]+)(@)", r":****\2", url, count=1)


def create_app() -> FastAPI:
    application = FastAPI(title=settings.app_name)
    application.include_router(api_router, prefix=settings.api_v1_prefix)

    @application.on_event("startup")
    async def log_db_target() -> None:
        url = get_settings().effective_database_url
        logging.info("DB 연결 대상: %s", _mask_database_url(url))

    @application.get("/")
    async def root() -> dict[str, str]:
        return {"app": settings.app_name, "docs": "/docs", "health": "/health"}

    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/ready")
    async def ready() -> dict[str, str]:
        return {"status": "ready"}

    return application


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run(
        "app.realtime.main:app",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "8000")),
    )


if __name__ == "__main__":
    run()
