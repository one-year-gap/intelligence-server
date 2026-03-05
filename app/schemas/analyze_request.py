"""분석 요청 DTO (job 단위)."""

from pydantic import Field

from app.schemas.base import SchemaBase


class AnalyzeRequest(SchemaBase):
    # 요청 추적 및 멱등성 키
    request_id: str = Field(..., alias="requestId", min_length=1, max_length=200)
    # 배치 실행 인스턴스 식별자
    job_instance_id: str = Field(..., alias="jobInstanceId", min_length=1, max_length=200)
    # 분석 버전 태그
    analysis_version: str = Field(..., alias="analysisVersion", min_length=1, max_length=50)
