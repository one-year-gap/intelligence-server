"""FastAPI application entrypoint. REST API + Kafka consumer runtime."""

if __package__ is None or __package__ == "":
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import FastAPI

from app.api.router import api_router
from app.api.v1.health import public_router as public_health_router
from app.core.config import get_settings
from app.core.logging import configure_logging

settings = get_settings()
configure_logging(settings.debug)

app = FastAPI(title=settings.app_name)
app.include_router(public_health_router)
app.include_router(api_router, prefix=settings.api_v1_prefix)
consumer_service = None


@app.get("/")
async def root() -> dict[str, str]:
    return {"app": settings.app_name, "docs": "/docs", "health": "/health"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@app.on_event("startup")
async def startup_event() -> None:
    global consumer_service
    if not settings.kafka_consumer_enabled:
        return
    from app.services.kafka_analysis_consumer_service import KafkaAnalysisConsumerService

    consumer_service = KafkaAnalysisConsumerService(settings)
    await consumer_service.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if consumer_service is not None:
        await consumer_service.stop()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
