"""Kafka subscriber for replication messages."""

import asyncio
from typing import Any, cast

import structlog
from aiokafka import ConsumerRecord, TopicPartition
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import KafkaMessage

from s3mer.backends.pool import BackendPool
from s3mer.common.errors import ErrorAction, ErrorClassifier
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker, NullMetricsTracker
from s3mer.config.settings import ReplicationMode
from s3mer.kafka.dlq import DlqPublisher
from s3mer.kafka.message_validation import (
    bind_request_id,
    parse_message,
    pause_partition,
    record_skip,
    resolve_operation,
)
from s3mer.kafka.replication_executor import replicate_operation
from s3mer.kafka.retry_scheduler import schedule_global_retry, schedule_per_backend_retry, track_background_task
from s3mer.kafka.subscribers_config import ReplicationRetryConfig

logger = get_logger(__name__)


def _consumer_record(msg: KafkaMessage) -> ConsumerRecord:
    raw = msg.raw_message
    return cast("ConsumerRecord", raw[0] if isinstance(raw, tuple) else raw)


def register_subscribers(
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    mode: ReplicationMode = ReplicationMode.BATCH,
    kafka_config: Any = None,
    metrics: MetricsTracker | None = None,
) -> None:
    """Register replication message subscriber(s) on the broker."""
    tracker = metrics or NullMetricsTracker()

    concurrency = 1
    dlq_enabled = True
    dlq_suffix = ".dlq"
    if kafka_config is not None:
        ReplicationRetryConfig.retry_delay = kafka_config.replication_retry_delay
        ReplicationRetryConfig.max_retry_delay = kafka_config.replication_max_retry_delay
        ReplicationRetryConfig.max_retries = kafka_config.replication_max_retries
        ReplicationRetryConfig.skip_if_etag_matches = kafka_config.replication_skip_if_etag_matches
        concurrency = kafka_config.concurrency
        dlq_enabled = kafka_config.dlq_enabled
        dlq_suffix = kafka_config.dlq_topic_suffix

    dlq = DlqPublisher(broker, topic, tracker, enabled=dlq_enabled, topic_suffix=dlq_suffix)

    tracker.set_replication_consumer_concurrency(concurrency)
    logger.info(
        "Replication consumer configured",
        mode=mode.value,
        concurrency=concurrency,
        max_retries=ReplicationRetryConfig.max_retries,
        retry_delay=ReplicationRetryConfig.retry_delay,
        max_retry_delay=ReplicationRetryConfig.max_retry_delay,
        skip_if_etag_matches=ReplicationRetryConfig.skip_if_etag_matches,
        dlq_enabled=dlq_enabled,
    )
    if concurrency > 1:
        logger.warning(
            "Replication concurrency > 1 may process messages out of order per partition; "
            "keep concurrency at 1 unless ordering guarantees are verified",
            concurrency=concurrency,
        )

    if mode == ReplicationMode.PER_BACKEND:
        logger.info("Registering isolated per-backend subscribers")
        for backend in pool.get_secondaries():
            backend_topic = f"{topic}.{backend.name}"
            _register_per_backend_subscriber(broker, backend_topic, pool, backend.name, kafka_config, tracker, dlq)
    else:
        logger.info("Registering batch subscriber")
        _register_batch_subscriber(broker, topic, pool, kafka_config, tracker, dlq)


def _register_batch_subscriber(  # noqa: PLR0915
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    kafka_config: Any = None,
    metrics: MetricsTracker | None = None,
    dlq: DlqPublisher | None = None,
) -> None:
    tracker = metrics or NullMetricsTracker()
    concurrency = kafka_config.concurrency if kafka_config is not None else 1
    subscriber = broker.subscriber(topic, group_id="s3mer-workers", max_workers=concurrency)

    @subscriber
    async def handle_batch_replication(msg_raw: str, msg: KafkaMessage) -> None:  # noqa: PLR0915
        record = _consumer_record(msg)
        partition = record.partition
        offset = record.offset

        request_id = bind_request_id(record)
        if request_id:
            structlog.contextvars.bind_contextvars(request_id=request_id)

        try:
            message = parse_message(msg_raw, tracker, "unknown", partition, offset)
            if message is None:
                return

            failed_tp = TopicPartition(topic, partition)
            operation = resolve_operation(
                message, tracker, ",".join(message.target_backends) or "unknown", partition, offset
            )
            if operation is None:
                return

            logger.info(
                "Processing batch replication message",
                message_id=message.message_id,
                partition=partition,
                offset=offset,
                operation=message.operation,
                targets=message.target_backends,
            )

            try:
                source = pool.get(message.source_backend)
            except KeyError as exc:
                await record_skip(
                    tracker,
                    message.operation,
                    ",".join(message.target_backends) or "unknown",
                    "skipped_poison",
                    message_id=message.message_id,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                    message=message,
                    dlq=dlq,
                    per_backend=False,
                )
                return

            failed_targets: list[str] = []
            last_exc: Exception | None = None
            for target_name in message.target_backends:
                try:
                    target = pool.get(target_name)
                except KeyError as exc:
                    await record_skip(
                        tracker,
                        message.operation,
                        target_name,
                        "skipped_poison",
                        message_id=message.message_id,
                        partition=partition,
                        offset=offset,
                        error=str(exc),
                        message=message,
                        dlq=dlq,
                        per_backend=False,
                    )
                    continue

                try:
                    await replicate_operation(operation, message, source, target, tracker)
                    tracker.record_replication_consumer_outcome(message.operation, target_name, "success")
                    logger.info("Replication succeeded", message_id=message.message_id, target=target_name)
                except Exception as exc:
                    action = ErrorClassifier.classify(exc)
                    if action == ErrorAction.FAIL:
                        await record_skip(
                            tracker,
                            message.operation,
                            target_name,
                            "skipped_permanent",
                            message_id=message.message_id,
                            partition=partition,
                            offset=offset,
                            error=str(exc),
                            message=message,
                            dlq=dlq,
                            per_backend=False,
                        )
                        continue

                    logger.exception(
                        "Replication failed",
                        message_id=message.message_id,
                        target=target_name,
                        error=str(exc),
                    )
                    failed_targets.append(target_name)
                    last_exc = exc

            if not failed_targets:
                return

            consumer = subscriber.consumer
            if consumer is None:
                logger.warning(
                    "Batch replication failed but consumer unavailable; offset will not commit",
                    message_id=message.message_id,
                    failed_targets=failed_targets,
                    partition=partition,
                    offset=offset,
                )
                tracker.record_replication_consumer_outcome(
                    message.operation, ",".join(failed_targets), "failed_no_consumer"
                )
                raise RuntimeError(
                    f"Replication failed for targets {failed_targets} and consumer is unavailable."
                ) from last_exc

            assigned = consumer.assignment()
            logger.warning(
                "Batch replication failed. Pausing all assigned partitions to prevent rebalance storm.",
                message_id=message.message_id,
                failed_targets=failed_targets,
                assigned_partitions=[(tp.topic, tp.partition) for tp in assigned],
                partition=partition,
                offset=offset,
            )
            for tp in assigned:
                pause_partition(consumer, tp, tracker)

            task = asyncio.create_task(
                schedule_global_retry(
                    subscriber=subscriber,
                    failed_tp=failed_tp,
                    failed_offset=offset,
                    assigned_partitions=assigned,
                    message=message,
                    operation=operation,
                    source=source,
                    failed_targets=failed_targets,
                    pool=pool,
                    metrics=tracker,
                    dlq=dlq,
                )
            )
            track_background_task(task)

            raise RuntimeError(f"Replication failed for targets {failed_targets}. Consumer paused.") from last_exc
        finally:
            structlog.contextvars.clear_contextvars()


def _register_per_backend_subscriber(
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    backend_name: str,
    kafka_config: Any = None,
    metrics: MetricsTracker | None = None,
    dlq: DlqPublisher | None = None,
) -> None:
    tracker = metrics or NullMetricsTracker()
    concurrency = kafka_config.concurrency if kafka_config is not None else 1
    subscriber = broker.subscriber(topic, group_id=f"s3mer-workers-{backend_name}", max_workers=concurrency)

    @subscriber
    async def handle_per_backend_replication(msg_raw: str, msg: KafkaMessage) -> None:
        record = _consumer_record(msg)
        partition = record.partition
        offset = record.offset

        request_id = bind_request_id(record)
        if request_id:
            structlog.contextvars.bind_contextvars(request_id=request_id)

        try:
            message = parse_message(msg_raw, tracker, backend_name, partition, offset)
            if message is None:
                return

            failed_tp = TopicPartition(topic, partition)
            operation = resolve_operation(message, tracker, backend_name, partition, offset)
            if operation is None:
                return

            logger.info(
                "Processing per-backend replication message",
                message_id=message.message_id,
                partition=partition,
                offset=offset,
                operation=message.operation,
                target=backend_name,
            )

            try:
                source = pool.get(message.source_backend)
                target = pool.get(backend_name)
            except KeyError as exc:
                await record_skip(
                    tracker,
                    message.operation,
                    backend_name,
                    "skipped_poison",
                    message_id=message.message_id,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                    message=message,
                    dlq=dlq,
                    per_backend=True,
                )
                return

            try:
                await replicate_operation(operation, message, source, target, tracker)
                tracker.record_replication_consumer_outcome(message.operation, backend_name, "success")
                logger.info("Replication succeeded", message_id=message.message_id, target=backend_name)
            except Exception as exc:
                action = ErrorClassifier.classify(exc)
                if action == ErrorAction.FAIL:
                    await record_skip(
                        tracker,
                        message.operation,
                        backend_name,
                        "skipped_permanent",
                        message_id=message.message_id,
                        partition=partition,
                        offset=offset,
                        error=str(exc),
                        message=message,
                        dlq=dlq,
                        per_backend=True,
                    )
                    return

                logger.exception(
                    "Replication failed",
                    message_id=message.message_id,
                    target=backend_name,
                    error=str(exc),
                )

                consumer = subscriber.consumer
                if consumer is None:
                    logger.warning(
                        "Per-backend replication failed but consumer unavailable; offset will not commit",
                        message_id=message.message_id,
                        target=backend_name,
                        partition=partition,
                        offset=offset,
                    )
                    tracker.record_replication_consumer_outcome(message.operation, backend_name, "failed_no_consumer")
                    raise RuntimeError(
                        f"Replication failed for target {backend_name} and consumer is unavailable."
                    ) from exc

                logger.warning(
                    "Per-backend replication failed. Pausing partition.",
                    message_id=message.message_id,
                    target=backend_name,
                    partition=partition,
                    offset=offset,
                )
                pause_partition(consumer, failed_tp, tracker)

                task = asyncio.create_task(
                    schedule_per_backend_retry(
                        subscriber=subscriber,
                        failed_tp=failed_tp,
                        failed_offset=offset,
                        message=message,
                        operation=operation,
                        source=source,
                        target=target,
                        metrics=tracker,
                        dlq=dlq,
                    )
                )
                track_background_task(task)

                raise RuntimeError(f"Replication failed for target {backend_name}. Partition paused.") from exc
        finally:
            structlog.contextvars.clear_contextvars()


# Backward-compatible re-exports for tests
_replicate_operation = replicate_operation
_schedule_global_retry = schedule_global_retry
_schedule_per_backend_retry = schedule_per_backend_retry
