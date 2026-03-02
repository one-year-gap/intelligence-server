"""FastAPI application entrypoint.

- 공개 헬스체크(`/health`, `/ready`)는 prefix 없이 노출
- 나머지 API는 `/api/v1` prefix로 노출
"""

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


@app.get("/")
def root() -> dict[str, str]:
    #Root 확인용 엔드 포인트
    return {"message": "counseling analytics api"}
