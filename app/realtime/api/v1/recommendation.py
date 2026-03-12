from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.schemas.recommendation import RecommendationRequest
from app.services.recommendation_service import run_recommendation_and_publish_to_kafka

router = APIRouter()


@router.post("/recommendations", status_code=202)
async def post_recommendations(
    body: RecommendationRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> Response:
    """
    202 Accepted 즉시 반환. 백그라운드에서 추천 생성 후 Kafka recommendation-topic 발행.
    Spring이 Kafka consume → persona_recommendation 적재 → CompletableFuture.complete(결과).
    """
    _ = session
    background_tasks.add_task(run_recommendation_and_publish_to_kafka, body.member_id)
    return Response(status_code=202, content=None)
