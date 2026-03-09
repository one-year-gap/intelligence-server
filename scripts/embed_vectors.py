"""
embedding_text를 DB에서 읽어 [업셀 포인트] 구간을 제거한 뒤 OpenAI 임베딩 API에 넘기고,
반환된 벡터를 product.embedding_vector에 UPDATE.
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

from openai import AsyncOpenAI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import SessionLocal

# API 호출 시 넣을 텍스트에서 제거할 블록 라벨 (포함 ~ 문자열 끝)
UPSELL_BLOCK_LABEL = "[업셀 포인트]"

# 배치 크기 (OpenAI 한 번에 보낼 건수)
EMBED_BATCH_SIZE = 50

# 테스트용: 처리할 상품 수 제한. 환경변수 EMBED_LIMIT=1 등으로 지정. 없으면 전부 처리.
def _get_embed_limit() -> int | None:
    v = os.environ.get("EMBED_LIMIT", "").strip()
    if not v:
        return None
    try:
        n = int(v)
        return n if n > 0 else None
    except ValueError:
        return None


def strip_upsell_from_embedding_text(embedding_text: str | None) -> str:
    """
    embedding_text에서 [업셀 포인트] ... 구간을 제거한 문자열 반환.
    API에 넘길 때만 사용. 없으면 원문 그대로.
    """
    if not embedding_text or not isinstance(embedding_text, str):
        return ""
    if UPSELL_BLOCK_LABEL not in embedding_text:
        return embedding_text.strip()
    before = embedding_text.split(UPSELL_BLOCK_LABEL)[0]
    return before.rstrip()


async def fetch_products_with_embedding_text(session: AsyncSession) -> list[dict[str, Any]]:
    """embedding_text가 비어 있지 않은 product_id, embedding_text 목록 조회."""
    result = await session.execute(
        text(
            "SELECT product_id, embedding_text FROM product "
            "WHERE embedding_text IS NOT NULL AND TRIM(embedding_text) != '' ORDER BY product_id"
        )
    )
    rows = result.fetchall()
    return [{"product_id": r[0], "embedding_text": r[1]} for r in rows]


async def get_embeddings_batch(
    client: AsyncOpenAI, texts: list[str], model: str
) -> list[list[float]]:
    """OpenAI embedding API 비동기 호출. texts 순서와 동일한 벡터 리스트 반환."""
    if not texts:
        return []
    resp = await client.embeddings.create(model=model, input=texts)
    order = {e.index: e.embedding for e in resp.data}
    return [order[i] for i in range(len(texts))]


def _build_bulk_update_vectors_sql(num_rows: int) -> str:
    """num_rows개 행을 한 번에 UPDATE하기 위한 VALUES 절 생성."""
    values = ", ".join(
        f"(:pid_{i}, :vec_{i}::vector)" for i in range(num_rows)
    )
    return (
        "UPDATE product AS p SET embedding_vector = v.vec FROM (VALUES "
        + values
        + ") AS v(pid, vec) WHERE p.product_id = v.pid"
    )


async def update_embedding_vectors(
    session: AsyncSession,
    product_id_and_vectors: list[tuple[int, list[float]]],
) -> int:
    """
    product_id별 embedding_vector를 한 번의 execute로 일괄 UPDATE.
    벡터는 list[float]로 전달. database.py에서 register_vector 등록된 연결 사용.
    """
    if not product_id_and_vectors:
        return 0

    n = len(product_id_and_vectors)
    update_sql = text(_build_bulk_update_vectors_sql(n))
    params = {}
    for i, (pid, vec) in enumerate(product_id_and_vectors):
        params[f"pid_{i}"] = pid
        params[f"vec_{i}"] = vec

    result = await session.execute(update_sql, params)
    await session.commit()
    return result.rowcount


async def main() -> None:
    settings = get_settings()
    client = AsyncOpenAI(api_key=settings.openai_api_key)
    model = settings.openai_embedding_model

    async with SessionLocal() as session:
        products = await fetch_products_with_embedding_text(session)
    if not products:
        print("embedding_text가 있는 상품이 없습니다.")
        return

    limit = _get_embed_limit()
    if limit is not None:
        products = products[:limit]
        print(f"테스트: 상품 {limit}건만 처리 (EMBED_LIMIT={limit})")
    print(f"대상 상품 수: {len(products)} (업셀 블록 제거 후 API 호출)")

    total_updated = 0
    for i in range(0, len(products), EMBED_BATCH_SIZE):
        batch = products[i : i + EMBED_BATCH_SIZE]
        texts_for_api = [
            strip_upsell_from_embedding_text(p["embedding_text"]) for p in batch
        ]
        # 빈 문자열 제거 시 해당 인덱스는 벡터 받지 않음 — 여기서는 빈 문자열도 API에 넘김(호출 수 유지). API는 빈 입력 시 에러 가능하므로 필터링.
        to_embed = [(p["product_id"], t) for p, t in zip(batch, texts_for_api) if t]
        if not to_embed:
            continue
        pids = [x[0] for x in to_embed]
        texts_only = [x[1] for x in to_embed]
        vectors = await get_embeddings_batch(client, texts_only, model)
        id_vec_pairs = list(zip(pids, vectors))

        async with SessionLocal() as session:
            n = await update_embedding_vectors(session, id_vec_pairs)
            total_updated += n
        print(f"  배치 {i // EMBED_BATCH_SIZE + 1}: {n}건 반영")

    print(f"완료: {total_updated}개 상품 embedding_vector 반영됨.")


if __name__ == "__main__":
    asyncio.run(main())
