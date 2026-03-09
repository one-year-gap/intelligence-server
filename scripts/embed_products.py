"""
상품 임베딩 파이프라인 (Step 1~5).
Step 1: Postgres에서 상품 공통 + 카테고리별 상세를 조회해 상품당 1 row로 정규화.
Step 2: tag_strategy.csv 로드 후 상품별 tags로 tag_contexts 주입.
Step 3: tag_contexts에서 [대상]/[추천 이유]/[참고] 있는 필드만 라벨 붙여 targeting_summary 생성.
Step 4: tag_contexts의 upsell_points를 합쳐 [업셀] 라벨로 upsell_summary 생성.
Step 5(embedding_text만): 공통+detail+targeting_summary로 embedding_text 생성 후 product 테이블에 UPDATE. 벡터 임베딩은 별도 파일.
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
from typing import Any

# 프로젝트 루트를 path에 넣어 app 모듈 import 가능하게 함
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import SessionLocal

# scripts/tag_strategy.csv 경로 (스크립트와 같은 디렉터리)
TAG_STRATEGY_CSV = os.path.join(_SCRIPT_DIR, "tag_strategy.csv")

# Java 레포 DDL 기준: product (product_id, name, product_type, ...) + 카테고리별 LEFT JOIN
# docs/product_schema.md 참고. one_line_summary, core_benefits 없음.
NORMALIZE_PRODUCTS_SQL = text("""
SELECT
    p.product_id,
    p.name,
    p.product_type,
    p.price,
    p.sale_price,
    p.discount_type,
    p.tags,
    COALESCE(mp.data_amount, tw.data_amount) AS data_amount,
    mp.tethering_sharing_data,
    COALESCE(mp.benefit_voice_call, tw.benefit_voice_call) AS benefit_voice_call,
    COALESCE(mp.benefit_sms, tw.benefit_sms) AS benefit_sms,
    mp.benefit_media,
    mp.benefit_premium,
    mp.benefit_signature_family_discount,
    mp.benefit_brands,
    i.speed,
    COALESCE(i.plan_title, ip.plan_title) AS plan_title,
    COALESCE(i.benefits, ip.benefits) AS benefits,
    ip.channel,
    a.addon_type,
    a.description
FROM product p
LEFT JOIN mobile_plan mp ON p.product_id = mp.product_id
LEFT JOIN internet i ON p.product_id = i.product_id
LEFT JOIN iptv ip ON p.product_id = ip.product_id
LEFT JOIN addon_service a ON p.product_id = a.product_id
LEFT JOIN tab_watch_plan tw ON p.product_id = tw.product_id
ORDER BY p.product_id
""")

# product_type별로 detail에 넣을 키만 정의. Step 2~5에서 product["detail"]로 타입별 필드 접근.
DETAIL_KEYS_BY_TYPE: dict[str, list[str]] = {
    "MOBILE_PLAN": [
        "data_amount",
        "tethering_sharing_data",
        "benefit_voice_call",
        "benefit_sms",
        "benefit_media",
        "benefit_premium",
        "benefit_signature_family_discount",
        "benefit_brands",
    ],
    "INTERNET": ["speed", "plan_title", "benefits"],
    "IPTV": ["channel", "plan_title", "benefits"],
    "ADDON": ["addon_type", "description"],
    "TAB_WATCH_PLAN": ["data_amount", "benefit_voice_call", "benefit_sms"],
}

# Step 5(embedding_text만): detail 블록 출력용 키별 라벨. embedding_text에는 targeting_summary·upsell_summary 모두 포함. 벡터 API 호출 시에만 업셀 제외 가능.
DETAIL_LABELS_BY_TYPE: dict[str, list[tuple[str, str]]] = {
    "MOBILE_PLAN": [
        ("data_amount", "[데이터량]"),
        ("tethering_sharing_data", "[테더링/공유]"),
        ("benefit_voice_call", "[혜택_음성]"),
        ("benefit_sms", "[혜택_문자]"),
        ("benefit_media", "[혜택_미디어]"),
        ("benefit_premium", "[혜택_프리미엄]"),
        ("benefit_signature_family_discount", "[혜택_시그니처가족할인]"),
        ("benefit_brands", "[혜택_브랜드]"),
    ],
    "INTERNET": [("speed", "[속도]"), ("plan_title", "[플랜명]"), ("benefits", "[혜택]")],
    "IPTV": [("channel", "[채널수]"), ("plan_title", "[플랜명]"), ("benefits", "[혜택]")],
    "ADDON": [("addon_type", "[부가유형]"), ("description", "[설명]")],
    "TAB_WATCH_PLAN": [
        ("data_amount", "[데이터량]"),
        ("benefit_voice_call", "[혜택_음성]"),
        ("benefit_sms", "[혜택_문자]"),
    ],
}


def build_embedding_text(product: dict[str, Any]) -> str:
    """
    상품 dict로 임베딩용 한 덩어리 문자열 생성. DB 저장용이므로 targeting_summary·upsell_summary 모두 포함.
    공통 + product_type별 detail + [추천 대상/상황] + [업셀 포인트]. (나중에 벡터 API 호출 시 업셀만 빼고 보낼 수 있음)
    """
    lines = []
    name = (product.get("name") or "").strip()
    if name:
        lines.append(f"[상품명] {name}")
    price = product.get("price")
    sale_price = product.get("sale_price")
    discount_type = (product.get("discount_type") or "").strip()
    if price is not None:
        line = f"[가격] {price}원"
        if sale_price is not None:
            line += f" (할인가 {sale_price}원)"
        if discount_type:
            line += f" 할인유형: {discount_type}"
        lines.append(line)
    tags = product.get("tags") or []
    if isinstance(tags, list):
        tag_str = ", ".join(str(t).strip() for t in tags if str(t).strip())
    else:
        tag_str = str(tags).strip()
    if tag_str:
        lines.append(f"[태그] {tag_str}")
    product_type = (product.get("product_type") or "").strip()
    if product_type:
        lines.append(f"[상품유형] {product_type}")

    detail = product.get("detail") or {}
    for key, label in DETAIL_LABELS_BY_TYPE.get(product_type, []):
        val = detail.get(key)
        if val is not None and str(val).strip():
            lines.append(f"{label} {str(val).strip()}")

    targeting = (product.get("targeting_summary") or "").strip()
    if targeting:
        lines.append(f"[추천 대상/상황] {targeting}")

    upsell = (product.get("upsell_summary") or "").strip()
    if upsell:
        lines.append(f"[업셀 포인트] {upsell}")

    return "\n".join(lines)


UPDATE_EMBEDDING_TEXT_SQL = text(
    "UPDATE product SET embedding_text = :txt WHERE product_id = :pid"
)


async def update_embedding_texts(
    session: AsyncSession,
    products: list[dict[str, Any]],
) -> int:
    """
    각 상품의 embedding_text를 DB product 테이블에 반영. product_id, embedding_text 키 필요.
    반환: 업데이트한 행 수.
    """
    count = 0
    for p in products:
        pid = p.get("product_id")
        txt = p.get("embedding_text")
        if pid is None:
            continue
        await session.execute(
            UPDATE_EMBEDDING_TEXT_SQL,
            {"pid": pid, "txt": txt if txt is not None else ""},
        )
        count += 1
    await session.commit()
    return count


def _row_to_normalized(row: Any) -> dict[str, Any]:
    """ResultProxy row를 정규화된 상품 dict로 변환. tags는 JSONB → list. detail은 product_type별 키만 포함."""
    raw = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    tags_raw = raw.get("tags")
    if tags_raw is None:
        tags: list[str] = []
    elif isinstance(tags_raw, str):
        tags = json.loads(tags_raw) if tags_raw else []
    else:
        tags = list(tags_raw) if tags_raw else []

    product_type_raw = raw.get("product_type")
    product_type = str(product_type_raw) if product_type_raw is not None else ""

    detail_keys = DETAIL_KEYS_BY_TYPE.get(product_type, [])
    detail = {k: raw.get(k) for k in detail_keys}

    return {
        "product_id": raw["product_id"],
        "product_type": product_type,
        "name": raw["name"],
        "price": raw["price"],
        "sale_price": raw["sale_price"],
        "discount_type": raw.get("discount_type"),
        "tags": tags,
        "detail": detail,
    }


async def fetch_normalized_products(session: AsyncSession) -> list[dict[str, Any]]:
    """
    Postgres에서 상품 공통 정보 + 카테고리별 상세를 조회해 상품당 1 row로 정규화된 리스트 반환.
    각 항목: 공통 필드(product_id, product_type, name, price, sale_price, discount_type, tags) + detail.
    detail은 product_type별로 해당 타입 전용 키만 포함 (DETAIL_KEYS_BY_TYPE 참고).
    """
    result = await session.execute(NORMALIZE_PRODUCTS_SQL)
    rows = result.fetchall()
    return [_row_to_normalized(r) for r in rows]


def load_tag_strategy(csv_path: str = TAG_STRATEGY_CSV) -> dict[str, dict[str, Any]]:
    """
    tag_strategy.csv를 로드해 tag_name 기준 딕셔너리 반환.
    반환: { tag_name: { "tag_group", "target_audience", "marketing_message", "upsell_points", "recommendation_hint", "related_tags", "caution", ... } }
    """
    strategy: dict[str, dict[str, Any]] = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("tag_name", "").strip()
            if name:
                strategy[name] = dict(row)
    return strategy


def inject_tag_contexts(
    products: list[dict[str, Any]],
    strategy: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    각 상품의 tags에 있는 태그명으로 strategy에서 row를 찾아 tag_contexts 리스트를 만들어 상품에 붙인 새 리스트 반환.
    산출: (정규화 상품 + "tag_contexts": [ {...}, ... ]) 리스트.
    """
    result = []
    for p in products:
        tag_contexts: list[dict[str, Any]] = []
        for tag_name in p.get("tags") or []:
            if isinstance(tag_name, str) and tag_name.strip():
                ctx = strategy.get(tag_name.strip())
                if ctx:
                    tag_contexts.append(ctx)
        out = {**p, "tag_contexts": tag_contexts}
        result.append(out)
    return result


# Step 3: 타겟팅 문장 — 있는 필드만 + 라벨. 순서: [대상] [추천 이유] [참고]
TARGETING_FIELDS = [
    ("target_audience", "[대상]"),
    ("recommendation_hint", "[추천 이유]"),
    ("caution", "[참고]"),
]
TARGETING_SEP_INNER = " · "  # 태그 내 필드 구분
TARGETING_SEP_BLOCKS = " | "  # 태그 블록 구분


def _build_one_tag_targeting(ctx: dict[str, Any]) -> str:
    """태그 하나의 target_audience, recommendation_hint, caution 중 값 있는 것만 라벨 붙여 이어 붙임."""
    parts = []
    for key, label in TARGETING_FIELDS:
        val = (ctx.get(key) or "").strip()
        if val:
            parts.append(f"{label} {val}")
    return TARGETING_SEP_INNER.join(parts)


def build_targeting_summaries(
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    각 상품의 tag_contexts에서 [대상] target_audience, [추천 이유] recommendation_hint, [참고] caution을
    있는 필드만 라벨 붙여 블록으로 만들고, 태그 블록은 | 로 이어서 targeting_summary 문자열 생성.
    """
    result = []
    for p in products:
        blocks = [_build_one_tag_targeting(ctx) for ctx in p.get("tag_contexts") or []]
        blocks = [b for b in blocks if b]
        targeting_summary = TARGETING_SEP_BLOCKS.join(blocks)
        result.append({**p, "targeting_summary": targeting_summary})
    return result


# Step 4: 업셀링 문장 — tag_contexts의 upsell_points 합침. 태그별 [업셀] 라벨 + | 구분
UPSELL_LABEL = "[업셀]"
UPSELL_SEP = " | "


def build_upsell_summaries(
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    각 상품의 tag_contexts에서 upsell_points를 모아 "다음으로 추천할 수 있는 옵션" 요약 문자열 생성.
    태그별로 값 있으면 [업셀] upsell_points, 블록은 | 로 이어서 upsell_summary 반환.
    """
    result = []
    for p in products:
        parts = []
        for ctx in p.get("tag_contexts") or []:
            val = (ctx.get("upsell_points") or "").strip()
            if val:
                parts.append(f"{UPSELL_LABEL} {val}")
        upsell_summary = UPSELL_SEP.join(parts)
        result.append({**p, "upsell_summary": upsell_summary})
    return result


async def main() -> None:
    """Step 1~4 후 embedding_text 생성·DB 반영. 임베딩 벡터 호출은 별도 파일에서."""
    async with SessionLocal() as session:
        products = await fetch_normalized_products(session)
    print(f"정규화된 상품 수: {len(products)}")

    strategy = load_tag_strategy()
    products_with_contexts = inject_tag_contexts(products, strategy)
    products_with_targeting = build_targeting_summaries(products_with_contexts)
    products_with_upsell = build_upsell_summaries(products_with_targeting)

    for p in products_with_upsell:
        p["embedding_text"] = build_embedding_text(p)

    async with SessionLocal() as session:
        updated = await update_embedding_texts(session, products_with_upsell)
    print(f"embedding_text DB 반영 완료: {updated}개 상품")

    if products_with_upsell:
        p0 = products_with_upsell[0]
        print("첫 상품 embedding_text:")
        print(p0.get("embedding_text", "")) 


if __name__ == "__main__":
    asyncio.run(main())
