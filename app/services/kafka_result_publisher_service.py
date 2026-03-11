import json
from typing import Any

from aiokafka import AIOKafkaProducer

from app.core.config import Settings
from app.infra.kafka.client_options import build_kafka_client_options


class KafkaResultPublisherService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            **build_kafka_client_options(self._settings),
        )
        try:
            await self._producer.start()
        except Exception:
            await self.stop()
            raise

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish_response_messages(self, payloads: list[dict[str, Any]]) -> int:
        return await self._publish_messages(
            topic=self._settings.kafka_analysis_response_topic,
            payloads=payloads,
            key_field="dispatchRequestId",
        )

    async def publish_response_message(self, payload: dict[str, Any]) -> None:
        await self._publish_message(
            topic=self._settings.kafka_analysis_response_topic,
            payload=payload,
            key_field="dispatchRequestId",
        )

    async def _publish_messages(self, topic: str, payloads: list[dict[str, Any]], key_field: str) -> int:
        if not payloads:
            return 0
        assert self._producer is not None

        for payload in payloads:
            await self._publish_message(topic, payload, key_field)
        return len(payloads)

    async def _publish_message(self, topic: str, payload: dict[str, Any], key_field: str) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(
            topic=topic,
            key=str(payload[key_field]),
            value=payload,
        )
