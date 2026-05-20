"""Replication message publisher — thin wrapper around FastStream publisher."""

import structlog
from faststream.kafka import KafkaBroker

from s3mer.common.logging import get_logger
from s3mer.kafka.messages import ReplicationMessage

logger = get_logger(__name__)


class ReplicationPublisher:
    """
    Publishes replication messages to the Kafka topic.

    Uses FastStream's KafkaBroker.publish() for reliable delivery.
    """

    def __init__(self, broker: KafkaBroker, topic: str) -> None:
        self._broker = broker
        self._topic = topic

    @property
    def topic(self) -> str:
        """Get the base topic name."""
        return self._topic

    async def publish(self, message: ReplicationMessage, topic: str | None = None) -> None:
        """
        Publish a replication message to Kafka.

        The message key is set to '{bucket}/{key}' to ensure
        operations on the same object go to the same partition
        (preserving order per-object).
        """
        # Partition key: ensures ordering per bucket/key
        key_parts = [message.bucket]
        if message.key:
            key_parts.append(message.key)
        partition_key = "/".join(key_parts)

        target_topic = topic or self._topic

        headers = {}
        request_id = structlog.contextvars.get_contextvars().get("request_id")
        if request_id:
            headers["x-s3mer-request-id"] = request_id

        await self._broker.publish(
            message=message.model_dump_json(),
            topic=target_topic,
            key=partition_key.encode(),
            headers=headers,
        )

        logger.debug(
            "Replication message published",
            message_id=message.message_id,
            operation=message.operation,
            topic=target_topic,
            key=partition_key,
        )
