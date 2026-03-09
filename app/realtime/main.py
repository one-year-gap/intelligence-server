"""FastAPI application entrypoint for realtime recommendation API."""

from __future__ import annotations

import os

from fastapi import FastAPI

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.realtime.api.router import api_router

settings = get_settings()
configure_logging(settings.debug)


def create_app() -> FastAPI:
    application = FastAPI(title=settings.app_name)
    application.include_router(api_router, prefix=settings.api_v1_prefix)

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
