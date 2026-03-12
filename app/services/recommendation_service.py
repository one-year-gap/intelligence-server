import json
import logging
import re
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from openai import AsyncOpenAI
from sqlalchemy import bindparam, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.schemas.recommendation import (
    RecommendedProductItem,
    RecommendationResponse,
    Segment,
)
from app.services.persona_recommendation_prompts import (
    build_user_prompt,
    format_products,
    get_persona_style_prompt,
    get_segment_system_prompt,
)
from app.services.retrieval_query_builder import build_retrieval_query_text

# member_llm_context 한 건 조회. LLM은 이 테이블만 보면 됨. (persona_type_id는 실제 테이블에 없을 수 있어 제외)
FETCH_MEMBER_LLM_CONTEXT_SQL = text("""
SELECT
    member_id, membership, age_group, join_months, children_count,
    family_group_num, family_role, persona_code, segment,
    current_subscriptions, current_product_types, product_type_clicks, current_data_usage_ratio,
    data_usage_pattern, churn_score, churn_tier, recent_counseling,
    recent_viewed_tags_top_3, contract_expiry_within_3m, updated_at
FROM member_llm_context
WHERE member_id = :member_id
""")

# 구독 중인 상품의 가격 조회 (segment별 가격 비교용). current_subscriptions의 product_id로 product 테이블 접근.
FETCH_SUBSCRIPTION_PRICES_SQL = text("""
SELECT product_id, price, sale_price, product_type
FROM product
WHERE product_id IN :ids
""").bindparams(bindparam("ids", expanding=True))

# 벡터 유사도 검색. query_vec은 list[float]로 전달. register_vector 등록된 연결에서 벡터 타입으로 바인딩됨.
SEARCH_SIMILAR_SQL = text("""
SELECT product_id
FROM product
WHERE embedding_vector IS NOT NULL
  AND (NOT (product_id = ANY(:exclude_ids)))
ORDER BY embedding_vector <#> :query_vec
LIMIT :k
""")

# product_type_clicks 기반 가중치: 클릭 많은 타입은 (거리 - 보너스)로 순위 상승. boost가 0이면 기존과 동일.
SEARCH_SIMILAR_WITH_TYPE_BOOST_SQL = text("""
SELECT product_id
FROM product
WHERE embedding_vector IS NOT NULL
  AND (NOT (product_id = ANY(:exclude_ids)))
ORDER BY (embedding_vector <#> :query_vec)
  - (CASE
       WHEN product_type = :boost_type1 THEN :boost1
       WHEN product_type = :boost_type2 THEN :boost2
       ELSE 0
     END)
LIMIT :k
""")

# 추천 후보 상품 상세 조회. data_amount는 OVER/UNDER 재정렬용 (mobile_plan·tab_watch_plan)
FETCH_PRODUCTS_FULL_SQL = text("""
SELECT
    p.product_id, p.name, p.product_type, p.price, p.sale_price, p.tags, p.embedding_text,
    COALESCE(mp.data_amount, tw.data_amount) AS data_amount
FROM product p
LEFT JOIN mobile_plan mp ON p.product_id = mp.product_id
LEFT JOIN tab_watch_plan tw ON p.product_id = tw.product_id
WHERE p.product_id IN :ids
""").bindparams(bindparam("ids", expanding=True))

# retrieval 후보 수 (LLM에 넣은 뒤 3개 선택)
RETRIEVAL_CANDIDATES_K = 7

# member_llm_context 미구축 시 사용할 기본 쿼리 텍스트
DEFAULT_RETRIEVAL_QUERY = "통신 요금제, 데이터 요금제, 부가서비스 추천"

# product.embedding_vector 및 쿼리 임베딩 공통 차원 (text-embedding-3-small 기본값). DB VECTOR(1536)과 동일해야 함.
# 상품 인덱싱(embedding_vector 저장)과 쿼리 임베딩은 반드시 동일 모델(openai_embedding_model) 사용.
EMBEDDING_DIMENSION = 1536

# 데이터무제한 판별용 태그 (포함 여부로 검사)
UNLIMITED_DATA_TAG_MARKER = "무제한"


def _normalize_embedding_for_db(embedding: list[float]) -> list[float]:
    """
    쿼리 임베딩을 DB VECTOR(1536)에 맞춤.
    - 1536이면 그대로 반환.
    - 1536 초과면 앞 1536개만 사용(다른 모델 사용 시 등).
    - 1536 미만이면 None 반환(패딩 불가, 검색 불가).
    """
    if len(embedding) == EMBEDDING_DIMENSION:
        return embedding
    if len(embedding) > EMBEDDING_DIMENSION:
        logging.warning(
            "임베딩 차원 초과: %d (기대 %d). 앞 %d개만 사용. product.embedding_vector와 동일 모델(openai_embedding_model) 사용 권장.",
            len(embedding),
            EMBEDDING_DIMENSION,
            EMBEDDING_DIMENSION,
        )
        return embedding[:EMBEDDING_DIMENSION]
    logging.error(
        "임베딩 차원 부족: %d (기대 %d). openai_embedding_model이 text-embedding-3-small인지, product 인덱싱과 동일 모델인지 확인.",
        len(embedding),
        EMBEDDING_DIMENSION,
    )
    return None


def _embedding_to_vector_str(embedding: list[float]) -> str:
    """OpenAI 임베딩 리스트를 pgvector 문자열 표현으로 변환 (로깅/폴백용)."""
    return "[" + ",".join(str(x) for x in embedding) + "]"


def _has_unlimited_data_tag(tags: list[str] | None) -> bool:
    """태그에 데이터무제한(또는 무제한) 포함 여부."""
    if not tags:
        return False
    return any(UNLIMITED_DATA_TAG_MARKER in (t or "") for t in tags)


def _reorder_by_data_usage_pattern(
    products: list[dict],
    data_usage_pattern: str | None,
) -> list[dict]:
    """
    data_usage_pattern에 따라 후보 순서 조정.
    OVER: 데이터무제한 태그·더 많은 data_amount 우선.
    UNDER: 데이터무제한 아닌 것·더 적은 data_amount 우선.
    FIT/None: 원순서 유지.
    """
    if not products:
        return products
    pattern = (data_usage_pattern or "").strip().upper()
    if pattern != "OVER" and pattern != "UNDER":
        return products

    def sort_key(p: dict) -> tuple:
        tags = _normalize_tags(p.get("tags"))
        has_unlimited = _has_unlimited_data_tag(tags)
        data_amount = p.get("data_amount")
        if data_amount is not None:
            try:
                amount = int(data_amount)
            except (TypeError, ValueError):
                amount = 0
        else:
            amount = None

        if pattern == "OVER":
            # 무제한 먼저, 그다음 data_amount 큰 순. amount 없으면 중간 취급(0).
            rank = 0 if has_unlimited else 1
            rev_amount = -(amount if amount is not None and amount > 0 else 0)
            return (rank, rev_amount)
        else:
            # UNDER: 무제한 아닌 것 먼저, 그다음 data_amount 작은 순. amount 없으면 뒤로
            rank = 1 if has_unlimited else 0
            amount_val = amount if amount is not None and amount >= 0 else 999999
            return (rank, amount_val)

    return sorted(products, key=sort_key)


def _normalize_tags(tags: list | str | None) -> list[str]:
    """DB JSONB tags를 list[str]로 통일."""
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    if isinstance(tags, str):
        try:
            parsed = json.loads(tags)
            return _normalize_tags(parsed)
        except json.JSONDecodeError:
            return [tags.strip()] if tags.strip() else []
    return []


async def _generate_recommendation_reasons(
    client: AsyncOpenAI,
    model: str,
    product_summaries: list[str],
) -> list[str]:
    """상품 요약 목록을 받아 각 상품별 추천 이유 한 문장씩 LLM 생성."""
    if not product_summaries:
        return []
    lines = [f"{i+1}. {s}" for i, s in enumerate(product_summaries)]
    product_list = "\n".join(lines)
    system_fallback = (
        "당신은 통신사 개인화 추천 AI입니다. 각 상품 추천 이유를 2~3문장으로 구체적으로 작성하세요. "
        "가격·혜택·태그·대상 고객 관점을 포함하면 좋습니다."
    )
    prompt = f"""아래 상품들을 고객에게 추천했습니다. 각 상품을 왜 추천했는지 2~3문장으로 구체적으로 설명해주세요.
상품 목록:
{product_list}

응답은 반드시 JSON만 주세요. 예시: {{"reasons": ["이유1(2~3문장)", "이유2(2~3문장)", "이유3(2~3문장)"]}}
"""
    try:
        resp = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_fallback},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        content = (resp.choices[0].message.content or "").strip()
        m = re.search(r"\{[\s\S]*\}", content)
        if m:
            data = json.loads(m.group())
            reasons = data.get("reasons") or []
            if isinstance(reasons, list):
                processed = [str(r).strip() or "고객님께 적합한 상품입니다." for r in reasons]
                if len(processed) < len(product_summaries):
                    processed.extend(
                        ["고객님께 적합한 상품입니다."] * (len(product_summaries) - len(processed))
                    )
                return processed[: len(product_summaries)]
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logging.warning("LLM 추천 이유 파싱 실패, 기본 문구 사용: %s", e, exc_info=True)
    return ["고객님께 적합한 상품입니다."] * len(product_summaries)


def _utc_now_iso() -> str:
    """ISO 8601 형식의 현재 UTC 시각 (Spring updatedAt 호환)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _exclude_ids_from_context(ctx: dict) -> list[int]:
    """current_subscriptions에서 product_id 목록 추출. DB 제외용."""
    raw = ctx.get("current_subscriptions")
    if not raw:
        return [0]
    if isinstance(raw, list):
        ids = []
        for x in raw:
            if isinstance(x, dict) and "product_id" in x:
                ids.append(int(x["product_id"]))
            elif isinstance(x, (int, float)):
                ids.append(int(x))
        return ids if ids else [0]
    if isinstance(raw, str):
        try:
            arr = json.loads(raw)
            return _exclude_ids_from_context({"current_subscriptions": arr})
        except json.JSONDecodeError:
            pass
    return [0]


# CHURN_RISK일 때 동일 product_type 내 현재 가격 대비 허용 상한 배율 (1.1 = 10% 초과까지)
CHURN_MAX_PRICE_RATIO = 1.1


async def _get_subscription_max_price_by_type(
    session: AsyncSession,
    ctx: dict,
) -> dict[str, int]:
    """
    current_subscriptions의 product_id로 product 테이블에서 가격·타입 조회 후
    product_type별 현재 최고 sale_price 반환.
    CHURN_RISK일 때 "한 상품 vs 같은 타입의 현재 한 상품" 비교용.
    """
    exclude_ids = _exclude_ids_from_context(ctx)
    if not exclude_ids or exclude_ids == [0]:
        return {}

    result = await session.execute(FETCH_SUBSCRIPTION_PRICES_SQL, {"ids": exclude_ids})
    by_type: dict[str, int] = {}
    for row in result.mappings():
        r = dict(row)
        ptype = (r.get("product_type") or "").strip()
        if not ptype:
            continue
        sale = r.get("sale_price")
        price = r.get("price")
        val = int(sale if sale is not None else price or 0)
        if ptype not in by_type or by_type[ptype] < val:
            by_type[ptype] = val
    return by_type


def _segment_enum(segment: str | None) -> Segment:
    """DB segment 문자열을 Segment enum으로."""
    if not segment:
        return Segment.normal
    s = (segment or "").strip().upper()
    if s == "CHURN_RISK":
        return Segment.churn_risk
    if s == "UPSELL":
        return Segment.upsell
    return Segment.normal


def _product_type_boost_from_ctx(ctx: dict) -> tuple[str, float, str, float]:
    """
    member_llm_context.product_type_clicks 상위 2개 타입에 대한 (type1, bonus1, type2, bonus2).
    bonus는 거리에서 빼서 해당 타입 순위 상승. 없으면 ("", 0.0, "", 0.0).
    """
    clicks = ctx.get("product_type_clicks")
    if not isinstance(clicks, dict) or not clicks:
        return ("", 0.0, "", 0.0)
    try:
        items = [(str(k).strip(), int(v)) for k, v in clicks.items() if v is not None and str(k).strip()]
        items.sort(key=lambda x: x[1], reverse=True)
        top2 = items[:2]
        if not top2:
            return ("", 0.0, "", 0.0)
        total = sum(c for _, c in top2)
        if total <= 0:
            return ("", 0.0, "", 0.0)
        # 최소 1개 타입은 있다고 가정. 두 번째 타입이 없으면 첫 번째 타입을 재사용하되 보너스는 0으로 둔다.
        if len(top2) >= 2:
            (t1, c1), (t2, c2) = top2[0], top2[1]
        else:
            (t1, c1) = top2[0]
            t2, c2 = t1, 0
        b1 = 0.15 * (c1 / total) if total > 0 else 0.0
        b2 = 0.08 * (c2 / total) if (total > 0 and c2 > 0) else 0.0
        return (t1, b1, t2, b2)
    except Exception:
        return ("", 0.0, "", 0.0)


async def _run_recommendation_with_context(
    session: AsyncSession,
    member_id: int,
    ctx: dict,
    settings: object,
    client: AsyncOpenAI,
) -> RecommendationResponse | None:
    """
    member_llm_context가 있을 때 사용하는 라이브 추천 경로.

    1) ctx 기반 retrieval 쿼리 텍스트를 만들고 임베딩을 생성한다.
    2) pgvector 유사도 검색을 수행한다. product_type_clicks가 있으면 타입별 가중치를 적용하고,
       CHURN_RISK인 경우 현재 구독 상품 가격 상한(type_caps)과 데이터 사용 패턴에 따라 후보를 재정렬/필터링한다.
    3) 후보 상품 목록을 LLM에 전달해 전반적인 마케팅 문구와 상품별 reason을 생성하고 RecommendationResponse로 변환한다.

    중간 단계(임베딩 생성, retrieval, LLM 호출)에서 실패하면 None을 반환하고 상위에서 폴백 경로를 사용한다.
    """
    top_k = getattr(settings, "recommend_top_k", 3)
    # 1) ctx 기반 쿼리 텍스트와 임베딩 생성
    query_text = build_retrieval_query_text(ctx)
    exclude_ids = _exclude_ids_from_context(ctx)

    # CHURN_RISK일 때 동일 product_type 기준 가격 상한용 (한 상품 vs 같은 타입 현재 가격)
    type_caps = await _get_subscription_max_price_by_type(session, ctx) if (ctx.get("segment") or "").strip().upper() == "CHURN_RISK" else {}

    try:
        emb_resp = await client.embeddings.create(
            model=settings.openai_embedding_model,
            input=query_text,
        )
        query_vec = emb_resp.data[0].embedding
    except Exception as e:
        logging.warning("ctx 기반 임베딩 실패: %s", e, exc_info=True)
        return None
    query_vec = _normalize_embedding_for_db(query_vec)
    if query_vec is None:
        return None

    # 2) pgvector 유사도 검색 (product_type_clicks 기반 가중치 포함)
    boost_type1, boost1, boost_type2, boost2 = _product_type_boost_from_ctx(ctx)
    use_type_boost = (boost1 > 0 or boost2 > 0)
    if use_type_boost:
        result = await session.execute(
            SEARCH_SIMILAR_WITH_TYPE_BOOST_SQL,
            {
                "query_vec": query_vec,
                "exclude_ids": exclude_ids,
                "k": RETRIEVAL_CANDIDATES_K,
                "boost_type1": boost_type1,
                "boost1": boost1,
                "boost_type2": boost_type2,
                "boost2": boost2,
            },
        )
    else:
        result = await session.execute(
            SEARCH_SIMILAR_SQL,
            {
                "query_vec": query_vec,
                "exclude_ids": exclude_ids,
                "k": RETRIEVAL_CANDIDATES_K,
            },
        )
    rows = result.fetchall()
    product_ids = [r[0] for r in rows]
    if not product_ids:
        return RecommendationResponse(
            segment=_segment_enum(ctx.get("segment")),
            cached_llm_recommendation="추천할 수 있는 상품이 없습니다.",
            recommended_products=[],
            source="LIVE",
            updated_at=_utc_now_iso(),
        )

    full_result = await session.execute(FETCH_PRODUCTS_FULL_SQL, {"ids": product_ids})
    id_to_row = {}
    for row in full_result.mappings():
        r = dict(row)
        id_to_row[r["product_id"]] = r
    products_ordered = [id_to_row[pid] for pid in product_ids if pid in id_to_row]
    logging.info(
        "recommendation: ctx retrieval 완료 member_id=%s 후보=%d",
        member_id,
        len(products_ordered),
    )

    # CHURN_RISK: 동일 product_type 내에서만 가격 비교. 현재 해당 타입 보유 시 그 타입 현재 최고가 * 1.1 초과 상품 제외.
    if type_caps:
        filtered = []
        for p in products_ordered:
            ptype = (p.get("product_type") or "").strip()
            sale = int(p.get("sale_price") or p.get("price") or 0)
            if ptype not in type_caps:
                filtered.append(p)  # 보유하지 않는 타입(예: ADDON)은 상한 없음
            elif sale <= int(type_caps[ptype] * CHURN_MAX_PRICE_RATIO):
                filtered.append(p)
        products_ordered = filtered

    # 데이터 사용량 패턴: OVER면 데이터무제한·많은 제공량 우선, UNDER면 적은 제공량 우선
    products_ordered = _reorder_by_data_usage_pattern(
        products_ordered,
        ctx.get("data_usage_pattern"),
    )

    if not products_ordered:
        return RecommendationResponse(
            segment=_segment_enum(ctx.get("segment")),
            cached_llm_recommendation="조건에 맞는 추천 상품이 없습니다.",
            recommended_products=[],
            source="LIVE",
            updated_at=_utc_now_iso(),
        )

    # 3) LLM 호출용 상품 포맷 정규화 (product_name 등 통일)
    for p in products_ordered:
        p["product_name"] = p.get("name") or ""
        p["product_price"] = int(p.get("price") or 0)
        p["sale_price"] = int(p.get("sale_price") or p.get("price") or 0)
        p["tags"] = _normalize_tags(p.get("tags"))

    products_text = format_products(products_ordered)
    segment = (ctx.get("segment") or "NORMAL").strip()
    persona_code = ctx.get("persona_code")
    system_prompt = get_segment_system_prompt(segment) + "\n\n" + get_persona_style_prompt(persona_code)
    user_prompt = build_user_prompt(ctx, products_text)

    json_instruction = (
        "응답은 반드시 다음 JSON 형식만 출력하세요. 다른 말 없이 JSON만.\n"
        "{"
        "\"cached_llm_recommendation\": \"전체 추천을 대표하는 전반적인 마케팅 문구를 2~4문장으로 작성하세요. "
        "고객의 세그먼트, 페르소나, 최근 상담 및 이용 패턴을 요약하고, 이번에 제안하는 상품 조합의 핵심 혜택과 가치를 자연스러운 카피 톤으로 설명하세요.\", "
        "\"recommended_products\": ["
        "{\"product_id\": 숫자, \"reason\": \"각 상품별로 2~3문장으로, 왜 이 고객에게 이 상품이 적합한지 구체적으로 설명하세요.\"}, ..."
        "]"
        "}"
    )
    logging.info("recommendation: ctx LLM 호출 member_id=%s 상품=%d", member_id, len(products_ordered[:top_k]))
    try:
        resp = await client.chat.completions.create(
            model=settings.openai_chat_model,
            messages=[
                {"role": "system", "content": system_prompt + "\n\n" + json_instruction},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        content = (resp.choices[0].message.content or "").strip()
        m = re.search(r"\{[\s\S]*\}", content)
        if not m:
            raise ValueError("JSON not found in response")
        data = json.loads(m.group())
        cached = (data.get("cached_llm_recommendation") or "").strip()
        if not cached:
            cached = "고객님의 이용 패턴과 관심사를 반영한 개인화 추천입니다."
        raw_list = data.get("recommended_products") or []
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logging.warning("LLM JSON 파싱 실패, 폴백: %s", e, exc_info=True)
        # 폴백: 상위 3개에 기본 reason
        raw_list = [
            {"product_id": p["product_id"], "reason": "고객님께 적합한 상품입니다."}
            for p in products_ordered[:top_k]
        ]
        cached = "고객님의 이용 패턴과 관심사를 반영한 요금제·부가서비스 추천입니다."

    recommended_products = []
    for rank, item in enumerate(raw_list[:top_k], 1):
        pid = item.get("product_id")
        reason = (item.get("reason") or "").strip() or "고객님께 적합한 상품입니다."
        if pid not in id_to_row:
            continue
        p = id_to_row[pid]
        tags = _normalize_tags(p.get("tags"))
        recommended_products.append(
            RecommendedProductItem(
                rank=rank,
                product_id=p["product_id"],
                product_name=(p.get("name") or "").strip(),
                product_type=(p.get("product_type") or "").strip(),
                product_price=int(p.get("price") or 0),
                sale_price=int(p.get("sale_price") or p.get("price") or 0),
                tags=tags,
                llm_reason=reason,
            )
        )

    return RecommendationResponse(
        segment=_segment_enum(ctx.get("segment")),
        cached_llm_recommendation=cached,
        recommended_products=recommended_products,
        source="LIVE",
        updated_at=_utc_now_iso(),
    )


async def _run_fallback_recommendation(
    client: AsyncOpenAI,
    settings: object,
    top_k: int,
) -> RecommendationResponse:
    """폴백: 새 세션(새 커넥션)에서 벡터 검색 + LLM. 중단된 트랜잭션 영향 없음."""
    logging.info("recommendation: 폴백 시작 (고정 쿼리 벡터 검색) top_k=%s", top_k)
    if SessionLocal is None:
        return RecommendationResponse(
            segment=Segment.normal,
            cached_llm_recommendation="DB 미설정으로 추천을 생성할 수 없습니다.",
            recommended_products=[],
            source="LIVE",
            updated_at=_utc_now_iso(),
        )
    async with SessionLocal() as fallback_session:
        try:
            emb_resp = await client.embeddings.create(
                model=settings.openai_embedding_model,
                input=DEFAULT_RETRIEVAL_QUERY,
            )
            query_vec = emb_resp.data[0].embedding
            logging.info("recommendation: 폴백 임베딩 완료")
        except Exception as e:
            logging.warning("recommendation: 폴백 임베딩 실패, 빈 추천 반환: %s", e, exc_info=True)
            return RecommendationResponse(
                segment=Segment.normal,
                cached_llm_recommendation="[일시 오류] 추천을 생성하지 못했습니다.",
                recommended_products=[],
                source="LIVE",
                updated_at=_utc_now_iso(),
            )
        query_vec = _normalize_embedding_for_db(query_vec)
        if query_vec is None:
            return RecommendationResponse(
                segment=Segment.normal,
                cached_llm_recommendation="임베딩 차원이 DB(VECTOR(1536))와 맞지 않습니다. openai_embedding_model과 상품 인덱싱 모델을 동일하게 설정하세요.",
                recommended_products=[],
                source="LIVE",
                updated_at=_utc_now_iso(),
            )

        result = await fallback_session.execute(
            SEARCH_SIMILAR_SQL,
            {
                "query_vec": query_vec,
                "exclude_ids": [0],
                "k": top_k,
            },
        )
        rows = result.fetchall()
        product_ids = [r[0] for r in rows]
        logging.info("recommendation: 폴백 벡터 검색 완료 product_ids=%s", product_ids[:10] if len(product_ids) > 10 else product_ids)
        if not product_ids:
            return RecommendationResponse(
                segment=Segment.normal,
                cached_llm_recommendation="추천할 수 있는 상품이 없습니다.",
                recommended_products=[],
                source="LIVE",
                updated_at=_utc_now_iso(),
            )

        full_result = await fallback_session.execute(FETCH_PRODUCTS_FULL_SQL, {"ids": product_ids})
        id_to_row = {}
        for row in full_result.mappings():
            r = dict(row)
            id_to_row[r["product_id"]] = r
        products_ordered = [id_to_row[pid] for pid in product_ids if pid in id_to_row]
        if not products_ordered:
            return RecommendationResponse(
                segment=Segment.normal,
                cached_llm_recommendation="추천 상품 정보를 불러오지 못했습니다.",
                recommended_products=[],
                source="LIVE",
                updated_at=_utc_now_iso(),
            )

        summaries = [
            f"{p.get('name') or ''} (product_id={p.get('product_id')})"
            for p in products_ordered
        ]
        logging.info("recommendation: 폴백 LLM reason 생성 요청 상품=%d", len(summaries))
        reasons = await _generate_recommendation_reasons(
            client,
            settings.openai_chat_model,
            summaries,
        )
        recommended_products = []
        for i, p in enumerate(products_ordered):
            tags = _normalize_tags(p.get("tags"))
            recommended_products.append(
                RecommendedProductItem(
                    rank=i + 1,
                    product_id=p["product_id"],
                    product_name=(p.get("name") or "").strip(),
                    product_type=(p.get("product_type") or "").strip(),
                    product_price=int(p.get("price") or 0),
                    sale_price=int(p.get("sale_price") or p.get("price") or 0),
                    tags=tags,
                    llm_reason=reasons[i] if i < len(reasons) else "고객님께 적합한 상품입니다.",
                )
            )
        return RecommendationResponse(
            segment=Segment.normal,
            cached_llm_recommendation="요금제·부가서비스 유사도와 LLM 기반 추천입니다.",
            recommended_products=recommended_products,
            source="LIVE",
            updated_at=_utc_now_iso(),
        )


async def run_recommendation_and_publish_to_kafka(member_id: int) -> None:
    """백그라운드: 추천 생성 후 Kafka 발행. Spring이 202 후 이 메시지를 consume해 DB 적재·CompletableFuture 완료."""
    try:
        resp = await get_recommendation(session=None, member_id=member_id)
        await publish_recommendation_to_kafka(member_id, resp)
    except Exception as e:
        logging.error("recommendation: 백그라운드 추천/Kafka 실패 member_id=%s: %s", member_id, e, exc_info=True)


async def publish_recommendation_to_kafka(
    member_id: int,
    response: RecommendationResponse,
) -> None:
    """추천 결과를 Kafka recommendation-topic으로 발행. Spring Consumer가 수신 후 persona_recommendation 적재."""
    settings = get_settings()
    topic = getattr(settings, "kafka_recommendation_topic", "recommendation")
    bootstrap = getattr(settings, "kafka_bootstrap_servers", "").strip()
    if not bootstrap:
        logging.warning("recommendation: Kafka 미설정, 발행 스킵 member_id=%s", member_id)
        return
    payload = {"memberId": member_id, **response.model_dump(by_alias=True)}
    producer = AIOKafkaProducer(
        bootstrap_servers=[s.strip() for s in bootstrap.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    try:
        await producer.start()
        await producer.send_and_wait(topic, value=payload, key=str(member_id).encode("utf-8"))
        logging.info("recommendation: Kafka 발행 완료 member_id=%s topic=%s", member_id, topic)
    except Exception as e:
        logging.error("recommendation: Kafka 발행 실패 member_id=%s: %s", member_id, e, exc_info=True)
    finally:
        await producer.stop()


class RecommendationService:
    """
    member_id 기준 추천 흐름을 담당하는 서비스.

    - member_llm_context를 조회해 고객 컨텍스트(ctx)를 만든다.
    - ctx가 있을 때는 ctx 기반 JIT RAG 파이프라인(임베딩 → 벡터 검색 → LLM)을 사용한다.
    - ctx가 없거나 ctx 경로에서 오류가 나면 폴백 벡터 검색 + LLM 경로로 안전하게 추천을 생성한다.
    """

    def __init__(self, settings: object, client: AsyncOpenAI) -> None:
        self.settings = settings
        self.client = client

    async def _load_member_context(
        self,
        session: AsyncSession,
        member_id: int,
    ) -> dict | None:
        """
        member_llm_context 한 건을 조회해 dict 형태 ctx로 변환한다.
        - 행이 없으면 None을 반환하고 폴백 경로를 사용한다.
        - 쿼리 오류가 발생하면 롤백 후 None을 반환한다.
        """
        try:
            ctx_result = await session.execute(
                FETCH_MEMBER_LLM_CONTEXT_SQL,
                {"member_id": member_id},
            )
            row = ctx_result.fetchone()
            if row is None:
                logging.info(
                    "recommendation: member_llm_context 행 없음 (member_id=%s). 테이블은 있으나 해당 회원 데이터 없음 → 폴백",
                    member_id,
                )
                return None
            return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        except (ProgrammingError, Exception) as e:
            logging.info(
                "recommendation: member_llm_context 조회 실패 → 폴백 예정 (member_id=%s, error=%s)",
                member_id,
                e,
            )
            try:
                await session.rollback()
            except Exception:
                pass
            return None

    async def _run_with_context(
        self,
        session: AsyncSession,
        member_id: int,
        ctx: dict,
    ) -> RecommendationResponse | None:
        """
        ctx가 존재할 때 ctx 기반 RAG 추천을 실행한다.
        내부 구현은 _run_recommendation_with_context에 위임한다.
        """
        return await _run_recommendation_with_context(
            session=session,
            member_id=member_id,
            ctx=ctx,
            settings=self.settings,
            client=self.client,
        )

    async def _run_fallback(self, top_k: int) -> RecommendationResponse:
        """
        ctx가 없거나 ctx 경로에서 오류가 난 경우 사용하는 폴백 추천 경로를 실행한다.
        내부 구현은 _run_fallback_recommendation에 위임한다.
        """
        return await _run_fallback_recommendation(
            client=self.client,
            settings=self.settings,
            top_k=top_k,
        )

    async def recommend_for_member(self, member_id: int) -> RecommendationResponse:
        """
        외부에서 사용하는 member_id 기준 추천 진입점.

        1) member_llm_context를 조회해 ctx를 만든다.
        2) ctx가 있으면 ctx 기반 RAG 추천을 시도한다.
        3) ctx가 없거나 ctx 경로에서 실패하면 폴백 벡터 검색 + LLM 경로로 추천을 생성한다.
        """
        logging.info("recommendation: 요청 시작 member_id=%s", member_id)
        top_k = getattr(self.settings, "recommend_top_k", 3)

        if SessionLocal is None:
            logging.warning("recommendation: DB 미설정, 빈 응답 반환 member_id=%s", member_id)
            return RecommendationResponse(
                segment=Segment.normal,
                cached_llm_recommendation="DB가 설정되지 않았습니다.",
                recommended_products=[],
                source="LIVE",
                updated_at=_utc_now_iso(),
            )

        async with SessionLocal() as worker_session:
            ctx: dict | None = await self._load_member_context(worker_session, member_id)

            if ctx:
                logging.info(
                    "recommendation: member_llm_context 사용 (member_id=%s, segment=%s, persona=%s)",
                    member_id,
                    (ctx.get("segment") or "").strip(),
                    (ctx.get("persona_code") or "").strip(),
                )
                try:
                    resp = await self._run_with_context(
                        session=worker_session,
                        member_id=member_id,
                        ctx=ctx,
                    )
                    if resp is not None:
                        logging.info(
                            "recommendation: ctx 경로 완료 member_id=%s segment=%s products=%s",
                            member_id,
                            resp.segment.value,
                            len(resp.recommended_products),
                        )
                        return resp
                except Exception as e:
                    logging.info(
                        "recommendation: ctx 기반 추천 실패 → 폴백 (member_id=%s, error=%s)",
                        member_id,
                        e,
                    )
                    try:
                        await worker_session.rollback()
                    except Exception:
                        pass

        logging.info(
            "recommendation: 폴백 경로 진입 (member_llm_context 없음 또는 ctx 추천 실패, member_id=%s)",
            member_id,
        )
        resp = await self._run_fallback(top_k=top_k)
        logging.info(
            "recommendation: 폴백 경로 완료 member_id=%s segment=%s products=%s",
            member_id,
            resp.segment.value,
            len(resp.recommended_products),
        )
        return resp


async def get_recommendation(
    session: AsyncSession | None,
    member_id: int,
) -> RecommendationResponse:
    """
    member_id 기준 추천 진입점.

    - DB 작업은 전용 세션(SessionLocal)만 사용하고, FastAPI 의존성 주입용 session 인자는 호환성만 유지한다.
    - 내부적으로 RecommendationService를 생성해 ctx 경로와 폴백 경로를 포함한 전체 RAG 파이프라인을 실행한다.
    """
    _ = session
    settings = get_settings()
    client = AsyncOpenAI(api_key=settings.openai_api_key)
    service = RecommendationService(settings=settings, client=client)
    return await service.recommend_for_member(member_id)
