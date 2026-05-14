"""Replication message publisher — thin wrapper around FastStream publisher."""

from __future__ import annotations

from typing import TYPE_CHECKING

from s3m.common.logging import get_logger
from s3m.kafka.messages import ReplicationMessage

if TYPE_CHECKING:
    from faststream.kafka import KafkaBroker

logger = get_logger(__name__)


class ReplicationPublisher:
    """
    Publishes replication messages to the Kafka topic.

    Uses FastStream's KafkaBroker.publish() for reliable delivery.
    """

    def __init__(self, broker: KafkaBroker, topic: str) -> None:
        self._broker = broker
        self._topic = topic

    async def publish(self, message: ReplicationMessage) -> None:
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

        await self._broker.publish(
            message=message.model_dump_json(),
            topic=self._topic,
            key=partition_key.encode(),
        )

        logger.debug(
            "Replication message published",
            message_id=message.message_id,
            operation=message.operation,
            topic=self._topic,
            key=partition_key,
        )
