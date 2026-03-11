from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class Segment(str, Enum):
    upsell = "UPSELL"
    churn_risk = "CHURN_RISK"
    normal = "NORMAL"


class RecommendationRequest(BaseModel):
    """POST /recommendations 요청. Body: {"memberId": number}"""
    member_id: int = Field(..., alias="memberId", description="회원 PK")

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)


class RecommendedProductItem(BaseModel):
    """명세 2.3: Spring/프론트 연동용 추천 상품 한 건. embedding_text는 내부용이라 응답에서 제외."""
    rank: int = Field(..., serialization_alias="rank")
    product_id: int = Field(..., serialization_alias="productId")
    product_name: str = Field(..., serialization_alias="productName")
    product_type: str = Field(..., serialization_alias="productType")
    product_price: int = Field(..., serialization_alias="productPrice")
    sale_price: int = Field(..., serialization_alias="salePrice")
    tags: list[str] = Field(..., serialization_alias="tags")
    llm_reason: str = Field(..., serialization_alias="llmReason")

    model_config = ConfigDict(serialize_by_alias=True)


class RecommendationResponse(BaseModel):
    segment: Segment
    cached_llm_recommendation: str = Field(..., serialization_alias="cachedLlmRecommendation")
    recommended_products: list[RecommendedProductItem] = Field(
        ..., serialization_alias="recommendedProducts"
    )
    source: str = Field(..., serialization_alias="source")  # CACHE | LIVE
    updated_at: str = Field(..., serialization_alias="updatedAt")  # ISO 8601

    model_config = ConfigDict(serialize_by_alias=True)
