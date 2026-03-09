from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.schemas.recommendation import RecommendationRequest, RecommendationResponse
from app.services.recommendation_service import get_recommendation

router = APIRouter()


@router.post("/recommendations", response_model=RecommendationResponse)
async def post_recommendations(
    body: RecommendationRequest,
    session: AsyncSession = Depends(get_db_session),
) -> RecommendationResponse:
    return await get_recommendation(
        session=session,
        member_id=body.member_id,
        profile_text=body.profile_text,
    )
