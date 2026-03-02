"""DI(의존성 주입) 팩토리 모음
"""

from functools import lru_cache
from app.core.config import get_settings
from app.infra.state.request_registry import RequestRegistry
from app.services.analyze_service import AnalyzeService
from app.services.idempotency_service import IdempotencyService

@lru_cache
def get_registry() -> RequestRegistry:
    """요청 처리 상태 저장소 생성"""
    return RequestRegistry()

@lru_cache
def get_analyze_service() -> AnalyzeService:
    """분석 오케스트레이션 서비스 생성"""
    settings = get_settings()
    registry = get_registry()

    return AnalyzeService(
        settings=settings,
        idempotency=IdempotencyService(registry),
    )
