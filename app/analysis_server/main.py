"""FastAPI entrypoint for the ephemeral analysis server."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.services.kafka_analysis_consumer_service import KafkaAnalysisConsumerService

settings = get_settings()
configure_logging(settings.debug)


@asynccontextmanager
async def lifespan(application: FastAPI):
    consumer_service = KafkaAnalysisConsumerService(settings)
    await consumer_service.start()
    application.state.analysis_consumer_service = consumer_service
    try:
        yield
    finally:
        await consumer_service.stop()


def create_app() -> FastAPI:
    application = FastAPI(title=f"{settings.app_name}-analysis-server", lifespan=lifespan)

    @application.get("/")
    async def root() -> dict[str, str]:
        return {"app": settings.app_name, "mode": "analysis-server", "health": "/health", "ready": "/ready"}

    @application.get("/health")
    async def health() -> dict[str, object]:
        return application.state.analysis_consumer_service.health_payload()

    @application.get("/ready")
    async def ready() -> dict[str, object]:
        payload = application.state.analysis_consumer_service.readiness_payload()
        if payload["ready"]:
            return payload
        raise HTTPException(status_code=503, detail=payload)

    return application


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run(
        "app.analysis_server.main:app",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "8000")),
    )


if __name__ == "__main__":
    run()
