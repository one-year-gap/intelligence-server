"""One-off Kafka batch entrypoint for keyword mapping/extraction."""

from __future__ import annotations

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infra.kafka.client_options import build_kafka_client_options
from app.infra.postgres.analysis_repository import AnalysisRepository
from app.infra.postgres.client import create_postgres_pool
from app.infra.postgres.dispatch_outbox_repository import DispatchOutboxRepository
from app.schemas.analysis_request_message import AnalysisRequestMessage
from app.services.kafka_analysis_consumer_service import KafkaAnalysisConsumerService
from app.services.sql_keyword_analysis_service import SqlKeywordAnalysisService

logger = logging.getLogger(__name__)


async def run_once() -> int:
    settings = get_settings()
    configure_logging(settings.debug)

    service = KafkaAnalysisConsumerService(settings)
    service._db_pool = await create_postgres_pool(settings)  # noqa: SLF001
    service._analysis_repository = AnalysisRepository(service._db_pool)  # noqa: SLF001
    service._outbox_repository = DispatchOutboxRepository(service._db_pool)  # noqa: SLF001
    service._analysis_service = SqlKeywordAnalysisService()  # noqa: SLF001

    consumer = AIOKafkaConsumer(
        settings.kafka_analysis_request_topic,
        group_id=settings.kafka_consumer_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        **build_kafka_client_options(settings),
    )

    processed_count = 0
    received_count = 0
    dropped_count = 0
    try:
        await consumer.start()
        polled = await consumer.getmany(
            timeout_ms=settings.kafka_poll_timeout_ms,
            max_records=settings.kafka_batch_size,
        )

        messages: list[AnalysisRequestMessage] = []
        for _, records in polled.items():
            for record in records:
                received_count += 1
                parsed = service._parse_message(record.value)  # noqa: SLF001
                if parsed is None:
                    dropped_count += 1
                    continue
                messages.append(parsed)

        if messages:
            for chunk in service._chunk(messages, settings.kafka_batch_size):  # noqa: SLF001
                await service._process_batch(chunk)  # noqa: SLF001
                processed_count += len(chunk)
            await consumer.commit()
        elif received_count > 0 and dropped_count == received_count:
            await consumer.commit()

        logger.info(
            "Keyword batch run finished once. received=%d dropped=%d processed=%d",
            received_count,
            dropped_count,
            processed_count,
        )
        return processed_count
    finally:
        await consumer.stop()
        if service._db_pool is not None:  # noqa: SLF001
            await service._db_pool.close()  # noqa: SLF001


def main() -> None:
    asyncio.run(run_once())


if __name__ == "__main__":
    main()
