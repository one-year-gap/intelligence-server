import asyncio
import json
import logging
from typing import Any
from collections.abc import Iterator

from aiokafka import AIOKafkaConsumer
from asyncpg import Pool
from pydantic import ValidationError

from app.core.config import Settings
from app.infra.postgres.analysis_repository import AnalysisRepository
from app.infra.postgres.client import create_postgres_pool
from app.infra.postgres.dispatch_outbox_repository import DispatchOutboxRepository
from app.schemas.analysis_request_message import AnalysisRequestMessage
from app.services.sql_keyword_analysis_service import SqlKeywordAnalysisService

logger = logging.getLogger(__name__)


class KafkaAnalysisConsumerService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        self._db_pool: Pool | None = None
        self._analysis_repository: AnalysisRepository | None = None
        self._outbox_repository: DispatchOutboxRepository | None = None
        self._analysis_service: SqlKeywordAnalysisService | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if not self._settings.kafka_consumer_enabled:
            logger.info("Kafka consumer is disabled. (KAFKA_CONSUMER_ENABLED=false)")
            return

        self._db_pool = await create_postgres_pool(self._settings)
        self._analysis_repository = AnalysisRepository(self._db_pool)
        self._outbox_repository = DispatchOutboxRepository(self._db_pool)
        self._analysis_service = SqlKeywordAnalysisService()

        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_analysis_request_topic,
            bootstrap_servers=[s.strip() for s in self._settings.kafka_bootstrap_servers.split(",") if s.strip()],
            group_id=self._settings.kafka_consumer_group_id,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._consume_loop(), name="kafka-analysis-consumer")

        logger.info(
            "Kafka consumer started. topic=%s group=%s batch_size=%d",
            self._settings.kafka_analysis_request_topic,
            self._settings.kafka_consumer_group_id,
            self._settings.kafka_batch_size,
        )

    async def stop(self) -> None:
        self._stop_event.set()

        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._db_pool:
            await self._db_pool.close()
            self._db_pool = None
        self._analysis_repository = None
        self._outbox_repository = None
        self._analysis_service = None

        logger.info("Kafka consumer stopped.")

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        assert self._analysis_repository is not None
        assert self._analysis_service is not None

        while not self._stop_event.is_set():
            try:
                polled = await self._consumer.getmany(
                    timeout_ms=self._settings.kafka_poll_timeout_ms,
                    max_records=self._settings.kafka_batch_size,
                )

                received_count = 0
                dropped_count = 0
                messages: list[AnalysisRequestMessage] = []
                for _, records in polled.items():
                    for record in records:
                        received_count += 1
                        message = self._parse_message(record.value)
                        if message is None:
                            dropped_count += 1
                            continue
                        messages.append(message)

                if messages:
                    for batch in self._chunk(messages, self._settings.kafka_batch_size):
                        await self._process_batch(batch)
                    await self._consumer.commit()
                    continue

                if received_count > 0 and dropped_count == received_count:
                    # 유효하지 않은 메시지만 들어온 경우엔 무한 재소비를 막기 위해 commit
                    await self._consumer.commit()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("Kafka consume loop failed. retry after short backoff.", exc_info=True)
                await asyncio.sleep(1.0)

    def _parse_message(self, payload: Any) -> AnalysisRequestMessage | None:
        try:
            return AnalysisRequestMessage.model_validate(payload)
        except ValidationError:
            logger.warning("Invalid analysis request message dropped. payload=%s", payload, exc_info=True)
            return None

    def _chunk(self, rows: list[AnalysisRequestMessage], size: int) -> Iterator[list[AnalysisRequestMessage]]:
        for idx in range(0, len(rows), size):
            yield rows[idx: idx + size]

    async def _process_batch(self, batch: list[AnalysisRequestMessage]) -> None:
        assert self._analysis_repository is not None
        assert self._outbox_repository is not None
        assert self._analysis_service is not None

        unique_request_ids = list(dict.fromkeys(msg.dispatch_request_id for msg in batch))
        acked_request_ids = await self._outbox_repository.mark_acked_by_request_ids(unique_request_ids)
        acked_count = len(acked_request_ids)

        unique_pairs = list(dict.fromkeys((msg.case_id, msg.analyzer_version) for msg in batch))
        case_ids = [pair[0] for pair in unique_pairs]
        analyzer_versions = [pair[1] for pair in unique_pairs]
        target_rows = await self._analysis_repository.find_targets_by_case_and_version(case_ids, analyzer_versions)
        target_by_pair = {
            (int(row["case_id"]), int(row["analyzer_version"])): row
            for row in target_rows
        }
        missing_pairs = [pair for pair in unique_pairs if pair not in target_by_pair]

        keyword_rows = await self._analysis_repository.load_active_keyword_rows()
        keyword_dict_rows = [dict(row) for row in keyword_rows]
        self._analysis_service.load_dictionary(keyword_dict_rows)
        keyword_name_by_id: dict[int, str] = {}
        for row in keyword_dict_rows:
            keyword_id = int(row["business_keyword_id"])
            if keyword_id not in keyword_name_by_id:
                keyword_name_by_id[keyword_id] = str(row["keyword_name"])

        targets = [dict(row) for row in target_rows]
        mapping_rows, completed_ids, failed_items = self._analysis_service.analyze_targets(targets)
        # DB write 권한은 Spring에만 있으므로 Python에서는 결과를 DB에 반영하지 않는다.
        # (business_keyword_mapping_result INSERT, consultation_analysis status UPDATE 미수행)

        if self._settings.kafka_log_each_message:
            self._log_message_outcomes(
                batch=batch,
                acked_request_ids=acked_request_ids,
                target_by_pair=target_by_pair,
                mapping_rows=mapping_rows,
                completed_ids=completed_ids,
                failed_items=failed_items,
                keyword_name_by_id=keyword_name_by_id,
            )

        logger.info(
            "Kafka batch consumed. messages=%d unique_requests=%d acked=%d unique_pairs=%d loaded_targets=%d missing_pairs=%d completed=%d failed=%d mappings=%d (only-outbox-write=enabled)",
            len(batch),
            len(unique_request_ids),
            acked_count,
            len(unique_pairs),
            len(target_rows),
            len(missing_pairs),
            len(completed_ids),
            len(failed_items),
            len(mapping_rows),
        )

    def _log_message_outcomes(
        self,
        batch: list[AnalysisRequestMessage],
        acked_request_ids: set[str],
        target_by_pair: dict[tuple[int, int], Any],
        mapping_rows: list[tuple[int, int, int]],
        completed_ids: list[int],
        failed_items: list[tuple[int, str]],
        keyword_name_by_id: dict[int, str],
    ) -> None:
        failed_by_analysis_id = {int(analysis_id): error for analysis_id, error in failed_items}
        completed_id_set = {int(analysis_id) for analysis_id in completed_ids}

        mapping_summary_by_analysis_id: dict[int, dict[str, int]] = {}
        mapping_detail_by_analysis_id: dict[int, dict[int, int]] = {}
        for analysis_id, _, count in mapping_rows:
            key = int(analysis_id)
            summary = mapping_summary_by_analysis_id.setdefault(
                key,
                {"keyword_types": 0, "keyword_hits": 0},
            )
            summary["keyword_types"] += 1
            summary["keyword_hits"] += int(count)
        for analysis_id, keyword_id, count in mapping_rows:
            detail = mapping_detail_by_analysis_id.setdefault(int(analysis_id), {})
            kid = int(keyword_id)
            detail[kid] = detail.get(kid, 0) + int(count)

        for message in batch:
            pair = (message.case_id, message.analyzer_version)
            target = target_by_pair.get(pair)
            acked = message.dispatch_request_id in acked_request_ids

            if target is None:
                logger.info(
                    "Kafka message outcome. request_id=%s case_id=%d analyzer_version=%d acked=%s status=MISSING_TARGET analysis_id=- keyword_types=0 keyword_hits=0",
                    message.dispatch_request_id,
                    message.case_id,
                    message.analyzer_version,
                    acked,
                )
                continue

            analysis_id = int(target["analysis_id"])
            summary = mapping_summary_by_analysis_id.get(analysis_id, {"keyword_types": 0, "keyword_hits": 0})
            detail = mapping_detail_by_analysis_id.get(analysis_id, {})
            error_message = failed_by_analysis_id.get(analysis_id)

            if error_message is not None:
                status = "FAILED"
            elif analysis_id in completed_id_set:
                status = "COMPLETED"
            else:
                status = "UNKNOWN"

            result_limit = max(1, self._settings.kafka_log_result_limit)
            result_items = [
                {
                    "keyword_id": keyword_id,
                    "keyword_name": keyword_name_by_id.get(keyword_id, "-"),
                    "count": cnt,
                }
                for keyword_id, cnt in sorted(detail.items(), key=lambda x: (-x[1], x[0]))[:result_limit]
            ]
            results_json = json.dumps(result_items, ensure_ascii=False)

            logger.info(
                "Kafka message outcome. request_id=%s case_id=%d analyzer_version=%d acked=%s status=%s analysis_id=%d keyword_types=%d keyword_hits=%d results=%s error=%s",
                message.dispatch_request_id,
                message.case_id,
                message.analyzer_version,
                acked,
                status,
                analysis_id,
                summary["keyword_types"],
                summary["keyword_hits"],
                results_json,
                error_message or "-",
            )
