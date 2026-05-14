"""FastStream Kafka broker setup."""

from __future__ import annotations

from faststream.kafka import KafkaBroker

from s3m.config.settings import KafkaConfig


def create_broker(config: KafkaConfig) -> KafkaBroker:
    """
    Create a FastStream KafkaBroker from configuration.

    The broker is shared between the proxy (publisher only)
    and the worker (subscriber).
    """
    return KafkaBroker(
        bootstrap_servers=config.bootstrap_servers,
    )
