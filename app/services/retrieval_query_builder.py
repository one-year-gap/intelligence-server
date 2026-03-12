"""
Phase 3: member_llm_context 한 행으로 retrieval용 쿼리 텍스트 생성.
구축/호출 시점에 사용하며, DB에는 저장하지 않음.
"""


def build_retrieval_query_text(ctx: dict) -> str:
    """
    고객 컨텍스트를 한 문장으로 이어서 임베딩용 쿼리 텍스트 생성.
    ctx: member_llm_context 행 (dict). 키는 DB 컬럼명.
    """
    age_group = (ctx.get("age_group") or "").strip()
    membership = (ctx.get("membership") or "").strip()
    join_months = ctx.get("join_months")
    join_str = f"{join_months}개월" if join_months is not None else ""
    current_types = ctx.get("current_product_types")
    if isinstance(current_types, dict):
        types_str = ", ".join(k for k, v in current_types.items() if v)
    else:
        types_str = str(current_types) if current_types else ""
    data_ratio = ctx.get("current_data_usage_ratio")
    usage_pattern = (ctx.get("data_usage_pattern") or "").strip()
    usage_str = f"데이터 사용량 {data_ratio}%로 {usage_pattern}" if data_ratio is not None else ""
    recent_tags = ctx.get("recent_viewed_tags_top_3")
    if isinstance(recent_tags, list):
        tags_str = ", ".join(str(t) for t in recent_tags[:3])
    else:
        tags_str = str(recent_tags) if recent_tags else ""
    segment = (ctx.get("segment") or "NORMAL").strip()
    persona_code = (ctx.get("persona_code") or "").strip()
    recent_counseling = (ctx.get("recent_counseling") or "").strip()[:500]

    product_type_clicks = ctx.get("product_type_clicks")
    top_clicked_types_str = ""
    if isinstance(product_type_clicks, dict):
        try:
            # 클릭 수 기준 내림차순 상위 2개 유형만 사용
            sorted_types = sorted(
                ((k, int(v)) for k, v in product_type_clicks.items() if v is not None),
                key=lambda kv: kv[1],
                reverse=True,
            )
            top_keys = [k for k, _ in sorted_types[:2]]
            if top_keys:
                top_clicked_types_str = ", ".join(top_keys)
        except Exception:
            top_clicked_types_str = ""

    parts = []
    if age_group or membership:
        parts.append(f"{age_group}, {membership} 고객")
    if join_str:
        parts.append(f"가입 {join_str}")
    if types_str:
        parts.append(f"현재 {types_str} 사용 중")
    if usage_str:
        parts.append(usage_str)
    if tags_str:
        parts.append(f"최근 {tags_str} 상품을 자주 조회함")
    if top_clicked_types_str:
        parts.append(f"최근 클릭이 많은 상품 유형: {top_clicked_types_str}")
    parts.append(f"segment={segment}, persona={persona_code or 'NONE'}")
    if recent_counseling:
        parts.append(f"최근 상담: {recent_counseling}")

    return " ".join(parts).strip() or "통신 요금제, 부가서비스 추천"
