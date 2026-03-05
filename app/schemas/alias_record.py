#별칭 JSONL 스키마
from pydantic import Field
from app.schemas.base import SchemaBase

class AliasItem(SchemaBase):
    #별칭 Id
    alias_id: int = Field(..., alias="aliasId", ge=1)
    #별칭 원문
    alias_text: str = Field(..., alias="aliasText", min_length=1, max_length=100)
    #별칭 정규화
    alias_norm: str = Field(..., alias="aliasNorm", min_length=1, max_length=100)


class AliasRecord(SchemaBase):
    #바즈니스 키워드 ID
    business_keyword_id: int = Field(..., alias="businessKeywordId", ge=1)
    #키워드 코드
    keyword_code: str = Field(..., alias="keywordCode", min_length=1, max_length=20)
    #키워드 이름
    keyword_name: str = Field(..., alias="keywordName", min_length=1, max_length=100)
    #별칭 목록들
    aliases: list[AliasItem] = Field(default_factory=list, min_length=1)