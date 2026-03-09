from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class Segment(str, Enum):
    upsell = "UPSELL"
    churn_risk = "CHURN_RISK"
    normal = "NORMAL"


class RecommendationRequest(BaseModel):
    member_id: int
    profile_text: str | None = None  # 테스트용: 있으면 이 텍스트와 유사한 상품 top-k 추천


class RecommendedProductItem(BaseModel):
    product_id: int = Field(..., serialization_alias="productId")
    reason: str

    model_config = ConfigDict(serialize_by_alias=True)


class RecommendationResponse(BaseModel):
    segment: Segment
    cached_llm_recommendation: str = Field(..., serialization_alias="cachedLlmRecommendation")
    recommended_products: list[RecommendedProductItem] = Field(
        ..., serialization_alias="recommendedProducts"
    )

    model_config = ConfigDict(serialize_by_alias=True)
