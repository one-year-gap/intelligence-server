# 요청 멱등성 처리

from app.core.constants import REQUEST_STATUS_PROCESSING
from app.infra.state.request_registry import RequestRegistry
from app.infra.state.lock import process_lock


class IdempotencyService:
    def __init__(self, registry: RequestRegistry) -> None:
        self.registry = registry

    def register_or_reject(self, request_id: str) -> bool:
        """처음 들어온 requestId면 등록하고 True, 중복이면 False."""
        # 락을 걸고 분석 처리 과정 진행
        with process_lock():
            return self.registry.create_if_absent(request_id, REQUEST_STATUS_PROCESSING)
