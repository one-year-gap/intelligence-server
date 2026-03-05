"""분석 결과 JSONL (상담 1건당 1줄)."""

from datetime import datetime

from pydantic import Field

from app.schemas.base import SchemaBase


class KeywordCountRecord(SchemaBase):
    # 비즈니스 키워드 식별자
    business_keyword_id: int = Field(..., alias="businessKeywordId", ge=1)
    # 사람이 읽기 쉬운 코드/이름
    keyword_code: str = Field(..., alias="keywordCode", min_length=1, max_length=20)
    keyword_name: str = Field(..., alias="keywordName", min_length=1, max_length=100)
    # 상담 1건 텍스트 안에서 발견된 총 횟수
    count: int = Field(..., ge=1, description="이 상담에서 해당 키워드가 발견된 횟수")


class ResultRecord(SchemaBase):
    request_id: str = Field(..., alias="requestId")
    job_instance_id: str = Field(..., alias="jobInstanceId")
    chunk_id: str = Field(..., alias="chunkId")

    case_id: int = Field(..., alias="caseId", ge=1)
    member_id: int = Field(..., alias="memberId", ge=1)

    # 상담 1건에서 키워드별 발견 횟수
    matched_keywords: list[KeywordCountRecord] = Field(default_factory=list, alias="matchedKeywords")
    analysis_version: str = Field(..., alias="analysisVersion")
    processed_at: datetime = Field(..., alias="processedAt")
