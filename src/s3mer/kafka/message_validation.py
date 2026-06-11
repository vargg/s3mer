"""Replication message validation and skip helpers."""

from typing import Any

from aiokafka import ConsumerRecord
from pydantic import ValidationError

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.kafka.dlq import DlqPublisher, ReplicationDeadLetter
from s3mer.kafka.messages import ReplicationMessage
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


def bind_request_id(record: ConsumerRecord) -> str | None:
    if not record.headers:
        return None
    for key, value in record.headers:
        if key == "x-s3mer-request-id":
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None


async def record_skip(
    metrics: MetricsTracker,
    operation: str,
    target_backend: str,
    outcome: str,
    *,
    message_id: str,
    partition: int,
    offset: int,
    error: str | None = None,
    message: ReplicationMessage | None = None,
    dlq: DlqPublisher | None = None,
    per_backend: bool = True,
) -> None:
    metrics.record_replication_consumer_outcome(operation, target_backend, outcome)
    logger.warning(
        "Replication skipped",
        message_id=message_id,
        operation=operation,
        target=target_backend,
        outcome=outcome,
        partition=partition,
        offset=offset,
        error=error,
    )
    if (
        dlq is not None
        and message is not None
        and outcome
        in (
            "skipped_permanent",
            "skipped_max_retries",
            "skipped_poison",
        )
    ):
        await dlq.publish(
            ReplicationDeadLetter(
                original_message=message,
                reason=outcome,
                error=error,
                target_backend=target_backend,
                partition=partition,
                offset=offset,
            ),
            per_backend=per_backend,
        )


def parse_message(
    msg_raw: str,
    metrics: MetricsTracker,
    target_backend: str,
    partition: int,
    offset: int,
) -> ReplicationMessage | None:
    try:
        return ReplicationMessage.model_validate_json(msg_raw)
    except ValidationError as exc:
        metrics.record_replication_consumer_outcome("unknown", target_backend, "skipped_poison")
        logger.warning(
            "Replication skipped",
            message_id="invalid",
            operation="unknown",
            target=target_backend,
            outcome="skipped_poison",
            partition=partition,
            offset=offset,
            error=str(exc),
        )
        return None


def resolve_operation(
    message: ReplicationMessage,
    metrics: MetricsTracker,
    target_backend: str,
    partition: int,
    offset: int,
) -> S3Operation | None:
    try:
        return S3Operation(message.operation)
    except ValueError as exc:
        metrics.record_replication_consumer_outcome(message.operation, target_backend, "skipped_poison")
        logger.warning(
            "Replication skipped",
            message_id=message.message_id,
            operation=message.operation,
            target=target_backend,
            outcome="skipped_poison",
            partition=partition,
            offset=offset,
            error=str(exc),
        )
        return None


def pause_partition(consumer: Any, tp: Any, metrics: MetricsTracker) -> None:
    consumer.pause(tp)
    metrics.record_replication_partition_paused(tp.topic, tp.partition)
    metrics.set_replication_paused_partition(tp.topic, tp.partition, paused=True)


def resume_partitions(consumer: Any, metrics: MetricsTracker, *partitions: Any) -> None:
    consumer.resume(*partitions)
    for tp in partitions:
        metrics.record_replication_partition_resumed(tp.topic, tp.partition)
        metrics.set_replication_paused_partition(tp.topic, tp.partition, paused=False)
