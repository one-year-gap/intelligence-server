from asyncpg import Pool


class DispatchOutboxRepository:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    async def load_metadata_by_request_ids(self, request_ids: list[str]) -> dict[str, dict[str, str | None]]:
        if not request_ids:
            return {}

        sql = """
        SELECT
            request_id,
            chunk_id,
            type::text AS type,
            dispatch_status::text AS dispatch_status
        FROM analysis_dispatch_outbox
        WHERE request_id = ANY($1::text[])
        """

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, request_ids)

        return {
            str(row["request_id"]): {
                "chunkId": row["chunk_id"],
                "type": row["type"],
                "dispatchStatus": row["dispatch_status"],
            }
            for row in rows
        }

    async def prepare_response_dispatch(self, request_id: str, analysis_status: str) -> bool:
        sql = """
        UPDATE analysis_dispatch_outbox
        SET
            type = 'RESPONSE'::dispatch_outbox_type,
            dispatch_status = 'SENT'::dispatch_status,
            analysis_status = $2::analysis_status,
            last_error = NULL,
            updated_at = NOW()
        WHERE request_id = $1
          AND dispatch_status <> 'ACKED'::dispatch_status
        RETURNING request_id
        """

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, request_id, analysis_status)

        return row is not None

    async def mark_response_retry(
        self,
        request_id: str,
        last_error: str,
        max_attempts: int,
        analysis_status: str,
    ) -> str:
        sql = """
        UPDATE analysis_dispatch_outbox
        SET
            type = 'RESPONSE'::dispatch_outbox_type,
            dispatch_status = CASE
                WHEN attempt_count + 1 >= $3 THEN 'DEAD'::dispatch_status
                ELSE 'RETRY'::dispatch_status
            END,
            analysis_status = $4::analysis_status,
            attempt_count = attempt_count + 1,
            next_retry_at = CASE
                WHEN attempt_count + 1 >= $3 THEN NULL
                ELSE NOW() + INTERVAL '5 minutes'
            END,
            last_error = LEFT($2, 1000),
            updated_at = NOW()
        WHERE request_id = $1
        RETURNING dispatch_status::text
        """

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, request_id, last_error, max_attempts, analysis_status)

        return str(row["dispatch_status"]) if row is not None else "RETRY"
