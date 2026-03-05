#상담 원문 JSONL 1 줄 단위
from datetime import datetime
from typing import Literal
from pydantic import Field
from app.schemas.base import SchemaBase


class CounselRecord(SchemaBase):
    #상담 데이터 Id
    case_id: int = Field(..., alias="caseId", ge=1)
    #멤버 Id
    member_id: int = Field(..., alias="memberId", ge=1)
    #상담 카테고리 Id
    category_code: str = Field(..., alias="categoryCode", min_length=1, max_length=20)
    #상담 제목
    title: str = Field(..., min_length=1, max_length=100)
    #상담 제목
    question_text: str = Field(..., alias="questionText", min_length=1)
    #상담 답변
    answer_text: str | None = Field(default=None, alias="answerText")
    #상담 상태
    status: Literal["OPEN", "SUPPORTING", "CLOSED"]