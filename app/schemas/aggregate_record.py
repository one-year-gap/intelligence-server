#개인별 집계 JSONL
from datetime import datetime
from pydantic import Field
from app.schemas.base import SchemaBase


class AggregateRecord(SchemaBase):
    request_id: str = Field(..., alias="requestId")
    #Batch Job Instance Id
    job_instance_id: int = Field(..., alias="jobInstanceId", ge=1)

    member_id: int = Field(..., alias="memberId", ge=1)

    business_keyword_id: int = Field(..., alias="businessKeywordId", ge=1)
    keyword_code: str = Field(..., alias="keywordCode", min_length=1, max_length=20)
    keyword_name: str = Field(..., alias="keywordName", min_length=1, max_length=100)

    count: int = Field(..., ge=0)

    # 어떤 상담에서 집계됐는지 추적용
    case_ids: list[int] = Field(default_factory=list, alias="caseIds")
