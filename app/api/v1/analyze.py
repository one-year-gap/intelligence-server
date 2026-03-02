"""분석 요청 API.

클라이언트가 요청하면 서비스 레이어에서
EFS 읽기 -> 매칭/집계 -> 결과 파일 저장까지 수행한다.
"""

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_analyze_service
from app.schemas.analyze_request import AnalyzeRequest
from app.schemas.analyze_response import AnalyzeResponse
from app.services.analyze_service import AnalyzeService

router = APIRouter()


@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(
    payload: AnalyzeRequest,
    service: AnalyzeService = Depends(get_analyze_service),
) -> AnalyzeResponse:
    """분석 실행
    - `requestId`가 이미 존재하면 `duplicated` 반환
    - 내부 예외는 500으로 반환
    """
    try:
        accepted, message = service.analyze(payload)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    if not accepted:
        return AnalyzeResponse(status="duplicated", requestId=payload.request_id, message=message)

    return AnalyzeResponse(status="accepted", requestId=payload.request_id, message=message)
