import json
import logging
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError
from pydantic import ValidationError

from app.core.config import Settings
from app.infra.kafka.client_options import build_kafka_client_options
from app.schemas.analysis_request_message import AnalysisRequestMessage

logger = logging.getLogger(__name__)


@dataclass
class KafkaPollResult:
    received_count: int
    dropped_count: int
    messages: list[AnalysisRequestMessage]


class KafkaRequestConsumerService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_analysis_request_topic,
            group_id=self._settings.kafka_consumer_group_id,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            max_poll_interval_ms=self._settings.kafka_max_poll_interval_ms,
            session_timeout_ms=self._settings.kafka_session_timeout_ms,
            heartbeat_interval_ms=self._settings.kafka_heartbeat_interval_ms,
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            **build_kafka_client_options(self._settings),
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def poll(self, max_records: int, timeout_ms: int) -> KafkaPollResult:
        assert self._consumer is not None
        polled = await self._consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=max_records,
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

        return KafkaPollResult(
            received_count=received_count,
            dropped_count=dropped_count,
            messages=messages,
        )

    async def commit(self) -> bool:
        assert self._consumer is not None
        try:
            await self._consumer.commit()
            return True
        except CommitFailedError:
            logger.warning(
                "Kafka commit failed due to rebalance. "
                "Increase KAFKA_MAX_POLL_INTERVAL_MS or reduce KAFKA_BATCH_SIZE. "
                "Current max_poll_interval_ms=%d batch_size=%d",
                self._settings.kafka_max_poll_interval_ms,
                self._settings.kafka_batch_size,
                exc_info=True,
            )
            return False

    def _parse_message(self, payload: Any) -> AnalysisRequestMessage | None:
        try:
            return AnalysisRequestMessage.model_validate(payload)
        except ValidationError:
            logger.warning("Invalid analysis request message dropped. payload=%s", payload, exc_info=True)
            return None
