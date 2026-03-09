from asyncpg import Pool, Record


class AnalysisRepository:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    async def find_targets_by_case_and_version(
        self,
        case_ids: list[int],
        analyzer_versions: list[int],
    ) -> list[Record]:
        if not case_ids:
            return []

        sql = """
        WITH input_pairs AS (
            SELECT *
            FROM unnest($1::bigint[], $2::bigint[]) AS t(case_id, analyzer_version)
        )
        SELECT
            ca.analysis_id,
            ca.case_id,
            ca.analyzer_version,
            sc.member_id,
            sc.title,
            sc.question_text
        FROM input_pairs ip
        JOIN consultation_analysis ca
          ON ca.case_id = ip.case_id
         AND ca.analyzer_version = ip.analyzer_version
        JOIN support_case sc
          ON sc.case_id = ca.case_id
        WHERE ca.analysis_status = 'IN_PROGRESS'::analysis_status
        """
        async with self._pool.acquire() as conn:
            return await conn.fetch(sql, case_ids, analyzer_versions)

    async def load_active_keyword_rows(self) -> list[Record]:
        sql = """
        SELECT
            bk.business_keyword_id,
            bk.keyword_code,
            bk.keyword_name,
            bka.alias_id,
            bka.alias_text,
            bka.alias_norm
        FROM business_keyword bk
        LEFT JOIN business_keyword_alias bka
          ON bka.business_keyword_id = bk.business_keyword_id
         AND bka.is_active = TRUE
        WHERE bk.is_active = TRUE
        ORDER BY bk.business_keyword_id, bka.alias_id
        """
        async with self._pool.acquire() as conn:
            return await conn.fetch(sql)
