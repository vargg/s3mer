"""Dead-letter queue for skipped replication messages."""

from datetime import UTC, datetime

from faststream.kafka import KafkaBroker
from pydantic import BaseModel, Field

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.kafka.messages import ReplicationMessage

logger = get_logger(__name__)


class ReplicationDeadLetter(BaseModel):
    """Audit record for a replication message that was skipped."""

    original_message: ReplicationMessage
    reason: str
    error: str | None = None
    target_backend: str
    partition: int
    offset: int
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


class DlqPublisher:
    """Publishes skipped replication tasks to a DLQ Kafka topic."""

    def __init__(
        self,
        broker: KafkaBroker,
        base_topic: str,
        metrics: MetricsTracker,
        *,
        enabled: bool = True,
        topic_suffix: str = ".dlq",
    ) -> None:
        self._broker = broker
        self._base_topic = base_topic
        self._metrics = metrics
        self._enabled = enabled
        self._topic_suffix = topic_suffix

    def topic_for(self, target_backend: str, *, per_backend: bool) -> str:
        if per_backend:
            return f"{self._base_topic}.{target_backend}{self._topic_suffix}"
        return f"{self._base_topic}{self._topic_suffix}"

    async def publish(
        self,
        entry: ReplicationDeadLetter,
        *,
        per_backend: bool,
    ) -> None:
        if not self._enabled:
            return

        topic = self.topic_for(entry.target_backend, per_backend=per_backend)
        key = f"{entry.original_message.bucket}/{entry.original_message.key or ''}"

        await self._broker.publish(
            message=entry.model_dump_json(),
            topic=topic,
            key=key.encode(),
        )
        self._metrics.record_replication_dlq(entry.reason)
        logger.warning(
            "Replication message sent to DLQ",
            message_id=entry.original_message.message_id,
            reason=entry.reason,
            target=entry.target_backend,
            topic=topic,
            partition=entry.partition,
            offset=entry.offset,
        )
