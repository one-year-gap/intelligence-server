from asyncpg import Pool


class DispatchOutboxRepository:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    async def mark_acked_by_request_ids(self, request_ids: list[str]) -> set[str]:
        if not request_ids:
            return set()

        sql = """
        UPDATE analysis_dispatch_outbox
        SET
            dispatch_status = 'ACKED'::dispatch_status,
            analysis_status = 'READY'::analysis_status,
            updated_at = NOW()
        WHERE request_id = ANY($1::text[])
        RETURNING request_id
        """

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, request_ids)

        return {str(row["request_id"]) for row in rows}
