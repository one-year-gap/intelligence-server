# 키워드 분석 응답 DTO
from typing import Literal
from pydantic import Field
from app.schemas.base import SchemaBase

class AnalyzeResponse(SchemaBase):
    status: Literal["accepted","rejected","duplicated"]
    request_id: str = Field(...,alias="requestId")
    message: str = Field(...,alias="message")