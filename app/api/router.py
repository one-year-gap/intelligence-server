#API 라우터 통합

from fastapi import APIRouter
from app.api.v1.analyze import router as analyze_router
from app.api.v1.health import router as health_router
from app.api.v1.ops import router as ops_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(analyze_router)
api_router.include_router(ops_router)
