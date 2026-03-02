"""운영/상태 조회 API."""

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_registry
from app.infra.state.request_registry import RequestRegistry

router = APIRouter(prefix="/ops")


@router.get("/requests/{request_id}")
def get_request_state(
    request_id: str,
    registry: RequestRegistry = Depends(get_registry),
) -> dict[str, str]:
    state = registry.get(request_id)
    if state is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="request not found")

    return {"requestId": request_id, **state}
