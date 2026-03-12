from __future__ import annotations

import asyncio
import time
from typing import Any

from aiokafka.abc import AbstractTokenProvider
from aiokafka.helpers import create_ssl_context
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from app.core.config import Settings


class MskIamTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str) -> None:
        self._region = region
        self._token = ""
        self._expiry_ms = 0
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        async with self._lock:
            now_ms = int(time.time() * 1000)
            if self._token and now_ms < self._expiry_ms - 60_000:
                return self._token

            token, expiry_ms = await asyncio.get_running_loop().run_in_executor(
                None,
                MSKAuthTokenProvider.generate_auth_token,
                self._region,
            )
            self._token = token
            self._expiry_ms = int(expiry_ms)
            return token


def build_kafka_client_options(settings: Settings) -> dict[str, Any]:
    options: dict[str, Any] = {
        "bootstrap_servers": [server.strip() for server in settings.kafka_bootstrap_servers.split(",") if server.strip()],
    }
    security_protocol = settings.kafka_security_protocol.strip().upper()
    if not security_protocol:
        return options

    options["security_protocol"] = security_protocol
    if security_protocol in {"SSL", "SASL_SSL"}:
        options["ssl_context"] = create_ssl_context()

    if security_protocol.startswith("SASL"):
        sasl_mechanism = settings.kafka_sasl_mechanism.strip().upper()
        if not sasl_mechanism:
            raise RuntimeError("KAFKA_SASL_MECHANISM must be set when using a SASL security protocol.")

        options["sasl_mechanism"] = sasl_mechanism
        if sasl_mechanism == "OAUTHBEARER":
            region = settings.kafka_aws_region.strip()
            if not region:
                raise RuntimeError("KAFKA_AWS_REGION or AWS_REGION must be set for SASL/OAUTHBEARER.")
            options["sasl_oauth_token_provider"] = MskIamTokenProvider(region)

    return options
