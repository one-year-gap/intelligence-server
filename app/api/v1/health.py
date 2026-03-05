"""헬스체크 API."""

from fastapi import APIRouter

from app.core.config import get_settings

router = APIRouter(tags=["health"])
public_router = APIRouter(tags=["health"])


def _health_payload(status: str) -> dict[str, str]:
    settings = get_settings()
    return {
        "status": status,
        "service": settings.app_name,
        "env": settings.app_env,
    }


@router.get("/health", summary="Liveness check")
def health() -> dict[str, str]:
    return _health_payload("ok")


@router.get("/ready", summary="Readiness check")
def ready() -> dict[str, str]:
    return _health_payload("ready")


@public_router.get("/health", include_in_schema=False)
def public_health() -> dict[str, str]:
    return _health_payload("ok")


@public_router.get("/ready", include_in_schema=False)
def public_ready() -> dict[str, str]:
    return _health_payload("ready")
