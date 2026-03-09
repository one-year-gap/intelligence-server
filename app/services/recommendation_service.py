import json
import logging
import re

from openai import AsyncOpenAI
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.schemas.recommendation import (
    RecommendedProductItem,
    RecommendationResponse,
    Segment,
)

# 프로필 텍스트와 유사한 상품 검색 (정규화된 벡터 기준 내적, <#> 사용)
SEARCH_SIMILAR_SQL = text("""
SELECT product_id
FROM product
WHERE embedding_vector IS NOT NULL
ORDER BY embedding_vector <#> :query_vec
LIMIT :k
""")

# IN 절에 리스트 바인딩 (expanding). ANY(:ids)는 드라이버에 따라 실패할 수 있어 IN 사용.
FETCH_PRODUCT_NAMES_SQL = text("""
SELECT product_id, name FROM product WHERE product_id IN :ids
""").bindparams(bindparam("ids", expanding=True))


async def _generate_recommendation_reasons(
    client: AsyncOpenAI,
    model: str,
    profile_text: str,
    product_names: list[tuple[int, str]],
) -> list[str]:
    """
    OpenAI로 각 상품을 왜 추천했는지 한 문장씩 생성. 실패 시 빈 문자열 또는 기본 문구 반환.
    """
    if not product_names:
        return []
    lines = [f"{i+1}. {name} (product_id={pid})" for i, (pid, name) in enumerate(product_names)]
    product_list = "\n".join(lines)
    prompt = f"""사용자 프로필: {profile_text}

아래 상품들을 이 프로필에 맞춰 추천했습니다. 각 상품을 왜 추천했는지 한 문장으로만 설명해주세요.
상품 목록:
{product_list}

응답은 반드시 JSON만 주세요. 다른 말 없이 예시 형식만 따르세요.
예시: {{"reasons": ["이유1", "이유2", "이유3"]}}
"""
    try:
        resp = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        content = (resp.choices[0].message.content or "").strip()
        # JSON 블록만 추출 (```json ... ``` 감싸진 경우 대비)
        m = re.search(r"\{[\s\S]*\}", content)
        if m:
            data = json.loads(m.group())
            reasons = data.get("reasons") or []
            if isinstance(reasons, list):
                processed_reasons = [str(r).strip() or "프로필과 유사한 상품입니다." for r in reasons]
                if len(processed_reasons) < len(product_names):
                    processed_reasons.extend(
                        ["프로필과 유사한 상품입니다."] * (len(product_names) - len(processed_reasons))
                return processed_reasons[: len(product_names)]
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logging.warning("LLM 추천 이유 파싱 실패, 기본 문구 사용: %s", e, exc_info=True)
    return ["프로필과 유사한 상품입니다."] * len(product_names)


async def get_recommendation(
    session: AsyncSession,
    member_id: int,
    profile_text: str | None = None,
) -> RecommendationResponse:
    settings = get_settings()
    top_k = settings.recommend_top_k

    if profile_text and profile_text.strip():
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        resp = await client.embeddings.create(
            model=settings.openai_embedding_model,
            input=profile_text.strip(),
        )
        query_vec = resp.data[0].embedding
        result = await session.execute(
            SEARCH_SIMILAR_SQL,
            {"query_vec": query_vec, "k": top_k},
        )
        rows = result.fetchall()
        product_ids = [row[0] for row in rows]
        if not product_ids:
            return RecommendationResponse(
                segment=Segment.normal,
                cached_llm_recommendation="[테스트] 추천할 상품이 없습니다.",
                recommended_products=[],
            )

        name_result = await session.execute(
            FETCH_PRODUCT_NAMES_SQL,
            {"ids": product_ids},
        )
        id_to_name = {r[0]: r[1] or "" for r in name_result.fetchall()}
        product_names = [(pid, id_to_name.get(pid, "")) for pid in product_ids]

        reasons = await _generate_recommendation_reasons(
            client,
            settings.openai_chat_model,
            profile_text.strip(),
            product_names,
        )
        recommended_products = [
            RecommendedProductItem(product_id=pid, reason=reasons[i])
            for i, pid in enumerate(product_ids)
        ]
        return RecommendationResponse(
            segment=Segment.normal,
            cached_llm_recommendation="[테스트] 프로필 텍스트와 벡터 유사도로 추천했고, 각 추천 이유는 LLM으로 생성했습니다.",
            recommended_products=recommended_products,
        )

    # profile_text 없으면 기존 스텁
    return RecommendationResponse(
        segment=Segment.normal,
        cached_llm_recommendation="[stub] 현재 사용 패턴 기반 추천입니다.",
        recommended_products=[
            RecommendedProductItem(product_id=1, reason="[stub] 테스트 상품입니다."),
        ],
    )
