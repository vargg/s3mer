"""Kafka subscriber for replication messages."""

import asyncio
from typing import Any, cast

import structlog
from aiokafka import ConsumerRecord, TopicPartition
from botocore.exceptions import ClientError
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import KafkaMessage
from pydantic import ValidationError

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.common.errors import ErrorAction, ErrorClassifier
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker, NullMetricsTracker
from s3mer.config.settings import ReplicationMode
from s3mer.kafka.messages import ReplicationMessage
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class ReplicationRetryConfig:
    """Replication retry configuration (set once from Kafka settings at startup)."""

    retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    max_retries: int = 10


_background_tasks: set[asyncio.Task[Any]] = set()


def register_subscribers(
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    mode: ReplicationMode = ReplicationMode.BATCH,
    kafka_config: Any = None,
    metrics: MetricsTracker | None = None,
) -> None:
    """
    Register the replication message subscriber(s) on the broker.

    Depending on the replication mode, registers either:
    1. A single consolidated batch subscriber.
    2. Isolated per-backend subscribers (one per secondary backend).
    """
    tracker = metrics or NullMetricsTracker()

    concurrency = 1
    if kafka_config is not None:
        ReplicationRetryConfig.retry_delay = kafka_config.replication_retry_delay
        ReplicationRetryConfig.max_retry_delay = kafka_config.replication_max_retry_delay
        ReplicationRetryConfig.max_retries = kafka_config.replication_max_retries
        concurrency = kafka_config.concurrency

    tracker.set_replication_consumer_concurrency(concurrency)
    logger.info(
        "Replication consumer configured",
        mode=mode.value,
        concurrency=concurrency,
        max_retries=ReplicationRetryConfig.max_retries,
        retry_delay=ReplicationRetryConfig.retry_delay,
        max_retry_delay=ReplicationRetryConfig.max_retry_delay,
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
            _register_per_backend_subscriber(broker, backend_topic, pool, backend.name, kafka_config, tracker)
    else:
        logger.info("Registering batch subscriber")
        _register_batch_subscriber(broker, topic, pool, kafka_config, tracker)


def _consumer_record(msg: KafkaMessage) -> ConsumerRecord:
    raw = msg.raw_message
    return cast("ConsumerRecord", raw[0] if isinstance(raw, tuple) else raw)


def _backend_name(client: S3BackendClient) -> str:
    return getattr(client, "name", "unknown")


def _bind_request_id(record: ConsumerRecord) -> str | None:
    if not record.headers:
        return None
    for key, value in record.headers:
        if key == "x-s3mer-request-id":
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None


def _retry_delay_seconds(attempt: int) -> float:
    return min(
        ReplicationRetryConfig.retry_delay * (2 ** (attempt - 1)),
        ReplicationRetryConfig.max_retry_delay,
    )


def _record_skip(
    metrics: MetricsTracker,
    operation: str,
    target_backend: str,
    outcome: str,
    *,
    message_id: str,
    partition: int,
    offset: int,
    error: str | None = None,
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


def _pause_partition(consumer: Any, tp: TopicPartition, metrics: MetricsTracker) -> None:
    consumer.pause(tp)
    metrics.record_replication_partition_paused(tp.topic, tp.partition)


def _resume_partitions(
    consumer: Any,
    metrics: MetricsTracker,
    *partitions: TopicPartition,
) -> None:
    consumer.resume(*partitions)
    for tp in partitions:
        metrics.record_replication_partition_resumed(tp.topic, tp.partition)


def _advance_consumer_offset(
    subscriber: Any,
    failed_tp: TopicPartition,
    failed_offset: int,
    metrics: MetricsTracker,
    *,
    resume_partitions: set[TopicPartition] | None = None,
) -> None:
    consumer = subscriber.consumer
    if consumer is None:
        msg = "Cannot advance consumer offset: consumer is not available"
        raise RuntimeError(msg)
    consumer.seek(failed_tp, failed_offset + 1)
    if resume_partitions:
        _resume_partitions(consumer, metrics, *resume_partitions)
    else:
        _resume_partitions(consumer, metrics, failed_tp)


def _register_batch_subscriber(  # noqa: PLR0915
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    kafka_config: Any = None,
    metrics: MetricsTracker | None = None,
) -> None:
    """
    Register a subscriber for the consolidated 'batch' replication mode.
    Consumes from the base topic and uses Consumer-Level Pausing on failure.
    """
    tracker = metrics or NullMetricsTracker()
    concurrency = kafka_config.concurrency if kafka_config is not None else 1
    subscriber = broker.subscriber(topic, group_id="s3mer-workers", max_workers=concurrency)

    @subscriber
    async def handle_batch_replication(msg_raw: str, msg: KafkaMessage) -> None:  # noqa: PLR0915
        record = _consumer_record(msg)
        partition = record.partition
        offset = record.offset

        request_id = _bind_request_id(record)
        if request_id:
            structlog.contextvars.bind_contextvars(request_id=request_id)

        try:
            try:
                message = ReplicationMessage.model_validate_json(msg_raw)
            except ValidationError as exc:
                _record_skip(
                    tracker,
                    "unknown",
                    "unknown",
                    "skipped_poison",
                    message_id="invalid",
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                return

            failed_tp = TopicPartition(topic, partition)

            logger.info(
                "Processing batch replication message",
                message_id=message.message_id,
                partition=partition,
                offset=offset,
                operation=message.operation,
                targets=message.target_backends,
            )

            try:
                operation = S3Operation(message.operation)
            except ValueError as exc:
                _record_skip(
                    tracker,
                    message.operation,
                    ",".join(message.target_backends) or "unknown",
                    "skipped_poison",
                    message_id=message.message_id,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                return

            try:
                source = pool.get(message.source_backend)
            except KeyError as exc:
                _record_skip(
                    tracker,
                    message.operation,
                    ",".join(message.target_backends) or "unknown",
                    "skipped_poison",
                    message_id=message.message_id,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                return

            failed_targets: list[str] = []
            last_exc: Exception | None = None
            for target_name in message.target_backends:
                try:
                    target = pool.get(target_name)
                except KeyError as exc:
                    _record_skip(
                        tracker,
                        message.operation,
                        target_name,
                        "skipped_poison",
                        message_id=message.message_id,
                        partition=partition,
                        offset=offset,
                        error=str(exc),
                    )
                    continue

                try:
                    await _replicate_operation(operation, message, source, target, tracker)
                    tracker.record_replication_consumer_outcome(message.operation, target_name, "success")
                    logger.info(
                        "Replication succeeded",
                        message_id=message.message_id,
                        target=target_name,
                    )
                except Exception as exc:
                    action = ErrorClassifier.classify(exc)
                    if action == ErrorAction.FAIL:
                        _record_skip(
                            tracker,
                            message.operation,
                            target_name,
                            "skipped_permanent",
                            message_id=message.message_id,
                            partition=partition,
                            offset=offset,
                            error=str(exc),
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
                _pause_partition(consumer, tp, tracker)

            task = asyncio.create_task(
                _schedule_global_retry(
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
                )
            )
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)

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
) -> None:
    """
    Register a subscriber for a specific secondary backend.
    Consumes from f"{topic}.{backend_name}" and uses Per-Backend Pausing on failure.
    """
    tracker = metrics or NullMetricsTracker()
    concurrency = kafka_config.concurrency if kafka_config is not None else 1
    subscriber = broker.subscriber(topic, group_id=f"s3mer-workers-{backend_name}", max_workers=concurrency)

    @subscriber
    async def handle_per_backend_replication(msg_raw: str, msg: KafkaMessage) -> None:
        record = _consumer_record(msg)
        partition = record.partition
        offset = record.offset

        request_id = _bind_request_id(record)
        if request_id:
            structlog.contextvars.bind_contextvars(request_id=request_id)

        try:
            try:
                message = ReplicationMessage.model_validate_json(msg_raw)
            except ValidationError as exc:
                _record_skip(
                    tracker,
                    "unknown",
                    backend_name,
                    "skipped_poison",
                    message_id="invalid",
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                return

            failed_tp = TopicPartition(topic, partition)

            logger.info(
                "Processing per-backend replication message",
                message_id=message.message_id,
                partition=partition,
                offset=offset,
                operation=message.operation,
                target=backend_name,
            )

            try:
                operation = S3Operation(message.operation)
            except ValueError as exc:
                _record_skip(
                    tracker,
                    message.operation,
                    backend_name,
                    "skipped_poison",
                    message_id=message.message_id,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                return

            try:
                source = pool.get(message.source_backend)
                target = pool.get(backend_name)
            except KeyError as exc:
                _record_skip(
                    tracker,
                    message.operation,
                    backend_name,
                    "skipped_poison",
                    message_id=message.message_id,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                return

            try:
                await _replicate_operation(operation, message, source, target, tracker)
                tracker.record_replication_consumer_outcome(message.operation, backend_name, "success")
                logger.info(
                    "Replication succeeded",
                    message_id=message.message_id,
                    target=backend_name,
                )
            except Exception as exc:
                action = ErrorClassifier.classify(exc)
                if action == ErrorAction.FAIL:
                    _record_skip(
                        tracker,
                        message.operation,
                        backend_name,
                        "skipped_permanent",
                        message_id=message.message_id,
                        partition=partition,
                        offset=offset,
                        error=str(exc),
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
                _pause_partition(consumer, failed_tp, tracker)

                task = asyncio.create_task(
                    _schedule_per_backend_retry(
                        subscriber=subscriber,
                        failed_tp=failed_tp,
                        failed_offset=offset,
                        message=message,
                        operation=operation,
                        source=source,
                        target=target,
                        metrics=tracker,
                    )
                )
                _background_tasks.add(task)
                task.add_done_callback(_background_tasks.discard)

                raise RuntimeError(f"Replication failed for target {backend_name}. Partition paused.") from exc
        finally:
            structlog.contextvars.clear_contextvars()


async def _schedule_global_retry(
    subscriber: Any,
    failed_tp: TopicPartition,
    failed_offset: int,
    assigned_partitions: set[TopicPartition],
    message: ReplicationMessage,
    operation: S3Operation,
    source: S3BackendClient,
    failed_targets: list[str],
    pool: BackendPool,
    metrics: MetricsTracker,
) -> None:
    """Background task to retry batch replication and resume all partitions."""
    attempt = 1
    while True:
        if attempt > ReplicationRetryConfig.max_retries:
            for target_name in failed_targets:
                _record_skip(
                    metrics,
                    message.operation,
                    target_name,
                    "skipped_max_retries",
                    message_id=message.message_id,
                    partition=failed_tp.partition,
                    offset=failed_offset,
                )
            logger.error(
                "Background retry: max retries exceeded, advancing consumer offset",
                message_id=message.message_id,
                failed_targets=failed_targets,
                max_retries=ReplicationRetryConfig.max_retries,
                partition=failed_tp.partition,
                offset=failed_offset,
            )
            try:
                _advance_consumer_offset(
                    subscriber,
                    failed_tp,
                    failed_offset,
                    metrics,
                    resume_partitions=assigned_partitions,
                )
            except Exception as e:
                logger.exception(
                    "Failed to advance consumer after max retries. Will retry advance in next loop.",
                    error=str(e),
                )
                attempt += 1
                continue
            break

        delay = _retry_delay_seconds(attempt)
        logger.info(
            "Background retry: waiting to retry replication",
            message_id=message.message_id,
            attempt=attempt,
            max_retries=ReplicationRetryConfig.max_retries,
            delay=delay,
            failed_targets=failed_targets,
        )
        await asyncio.sleep(delay)

        still_failed: list[str] = []
        for target_name in failed_targets:
            metrics.record_replication_retry(message.operation, target_name)
            target = pool.get(target_name)
            try:
                await _replicate_operation(operation, message, source, target, metrics)
                metrics.record_replication_consumer_outcome(message.operation, target_name, "success")
                logger.info(
                    "Background retry: replication succeeded",
                    message_id=message.message_id,
                    target=target_name,
                    attempt=attempt,
                )
            except Exception as exc:
                action = ErrorClassifier.classify(exc)
                if action == ErrorAction.FAIL:
                    _record_skip(
                        metrics,
                        message.operation,
                        target_name,
                        "skipped_permanent",
                        message_id=message.message_id,
                        partition=failed_tp.partition,
                        offset=failed_offset,
                        error=str(exc),
                    )
                    continue

                logger.exception(
                    "Background retry: replication failed",
                    message_id=message.message_id,
                    target=target_name,
                    attempt=attempt,
                    error=str(exc),
                )
                still_failed.append(target_name)

        if not still_failed:
            logger.info(
                "Background retry: all replication tasks succeeded. Resuming consumer.",
                message_id=message.message_id,
                partition=failed_tp.partition,
                offset=failed_offset,
            )
            try:
                subscriber.consumer.seek(failed_tp, failed_offset)
                _resume_partitions(subscriber.consumer, metrics, *assigned_partitions)
            except Exception as e:
                logger.exception(
                    "Failed to resume consumer partitions. Will retry in next loop.",
                    error=str(e),
                )
                attempt += 1
                continue
            break

        failed_targets = still_failed
        attempt += 1


async def _schedule_per_backend_retry(
    subscriber: Any,
    failed_tp: TopicPartition,
    failed_offset: int,
    message: ReplicationMessage,
    operation: S3Operation,
    source: S3BackendClient,
    target: S3BackendClient,
    metrics: MetricsTracker,
) -> None:
    """Background task to retry per-backend replication and resume the partition."""
    attempt = 1
    while True:
        if attempt > ReplicationRetryConfig.max_retries:
            _record_skip(
                metrics,
                message.operation,
                target.name,
                "skipped_max_retries",
                message_id=message.message_id,
                partition=failed_tp.partition,
                offset=failed_offset,
            )
            logger.error(
                "Background retry: max retries exceeded, advancing consumer offset",
                message_id=message.message_id,
                target=target.name,
                max_retries=ReplicationRetryConfig.max_retries,
                partition=failed_tp.partition,
                offset=failed_offset,
            )
            try:
                _advance_consumer_offset(subscriber, failed_tp, failed_offset, metrics)
            except Exception as e:
                logger.exception(
                    "Failed to advance consumer after max retries. Will retry advance in next loop.",
                    error=str(e),
                )
                attempt += 1
                continue
            break

        delay = _retry_delay_seconds(attempt)
        logger.info(
            "Background retry: waiting to retry replication",
            message_id=message.message_id,
            target=target.name,
            attempt=attempt,
            max_retries=ReplicationRetryConfig.max_retries,
            delay=delay,
        )
        await asyncio.sleep(delay)
        metrics.record_replication_retry(message.operation, target.name)

        try:
            await _replicate_operation(operation, message, source, target, metrics)
            metrics.record_replication_consumer_outcome(message.operation, target.name, "success")
            logger.info(
                "Background retry: replication succeeded. Resuming partition.",
                message_id=message.message_id,
                target=target.name,
                attempt=attempt,
            )
            subscriber.consumer.seek(failed_tp, failed_offset)
            _resume_partitions(subscriber.consumer, metrics, failed_tp)
            break
        except Exception as exc:
            action = ErrorClassifier.classify(exc)
            if action == ErrorAction.FAIL:
                _record_skip(
                    metrics,
                    message.operation,
                    target.name,
                    "skipped_permanent",
                    message_id=message.message_id,
                    partition=failed_tp.partition,
                    offset=failed_offset,
                    error=str(exc),
                )
                try:
                    _advance_consumer_offset(subscriber, failed_tp, failed_offset, metrics)
                except Exception as e:
                    logger.exception("Failed to advance consumer after permanent failure.", error=str(e))
                    attempt += 1
                    continue
                break

            logger.exception(
                "Background retry: replication failed",
                message_id=message.message_id,
                target=target.name,
                attempt=attempt,
                error=str(exc),
            )
        attempt += 1


async def _replicate_operation(  # noqa: PLR0912, PLR0915 - Centralized match dispatcher with error handling
    operation: S3Operation,
    message: ReplicationMessage,
    source: S3BackendClient,
    target: S3BackendClient,
    metrics: MetricsTracker | None = None,
) -> None:
    """
    Replicate a single S3 operation from source to target backend.

    For PutObject, reads the object from the source and writes it to the target.
    For other operations, replays the operation directly on the target.
    """
    tracker = metrics or NullMetricsTracker()
    target_name = _backend_name(target)

    def _skip_source_gone(reason: str, error: ClientError) -> None:
        tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_source_gone")
        logger.warning(
            reason,
            bucket=message.bucket,
            key=message.key,
            message_id=message.message_id,
            target=target_name,
            error=str(error),
        )

    match operation:
        case S3Operation.PUT_OBJECT:
            try:
                get_response = await source.execute(
                    S3Operation.GET_OBJECT,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchKey", "NoSuchBucket"):
                    _skip_source_gone(
                        "Source object or bucket no longer exists, skipping replication",
                        e,
                    )
                    return
                raise

            body = get_response["Body"]
            put_params: dict = {
                "Bucket": message.bucket,
                "Key": message.key,
                "Body": body,
            }

            if "ContentType" in message.metadata:
                put_params["ContentType"] = message.metadata["ContentType"]

            content_length = message.metadata.get("ContentLength")
            if content_length is None:
                try:
                    head_resp = await source.execute(
                        S3Operation.HEAD_OBJECT,
                        {"Bucket": message.bucket, "Key": message.key},
                    )
                    content_length = head_resp["ContentLength"]
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code in ("NoSuchKey", "NoSuchBucket"):
                        _skip_source_gone(
                            "Source object or bucket no longer exists on HEAD, skipping replication",
                            e,
                        )
                        return
                    raise

            put_params["ContentLength"] = int(content_length)

            async with body:
                await target.execute(S3Operation.PUT_OBJECT, put_params)

        case S3Operation.CREATE_BUCKET:
            await target.execute(
                S3Operation.CREATE_BUCKET,
                {"Bucket": message.bucket},
            )

        case S3Operation.DELETE_BUCKET:
            await target.execute(
                S3Operation.DELETE_BUCKET,
                {"Bucket": message.bucket},
            )

        case S3Operation.DELETE_OBJECT:
            try:
                await target.execute(
                    S3Operation.DELETE_OBJECT,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchKey":
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Object already absent on target, treating delete as success",
                        bucket=message.bucket,
                        key=message.key,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.PUT_OBJECT_TAGGING:
            try:
                tag_response = await source.execute(
                    S3Operation.GET_OBJECT_TAGGING,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchKey", "NoSuchBucket"):
                    _skip_source_gone(
                        "Source object or bucket no longer exists for tagging, skipping replication",
                        e,
                    )
                    return
                raise
            await target.execute(
                S3Operation.PUT_OBJECT_TAGGING,
                {
                    "Bucket": message.bucket,
                    "Key": message.key,
                    "Tagging": {"TagSet": tag_response.get("TagSet", [])},
                },
            )

        case S3Operation.DELETE_OBJECT_TAGGING:
            try:
                await target.execute(
                    S3Operation.DELETE_OBJECT_TAGGING,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchKey":
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Object already absent on target, treating delete tagging as success",
                        bucket=message.bucket,
                        key=message.key,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.PUT_BUCKET_LIFECYCLE:
            try:
                lifecycle_resp = await source.execute(
                    S3Operation.GET_BUCKET_LIFECYCLE,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchLifecycleConfiguration"):
                    _skip_source_gone(
                        "Source bucket or lifecycle configuration no longer exists, skipping replication",
                        e,
                    )
                    return
                raise
            await target.execute(
                S3Operation.PUT_BUCKET_LIFECYCLE,
                {
                    "Bucket": message.bucket,
                    "LifecycleConfiguration": {"Rules": lifecycle_resp.get("Rules", [])},
                },
            )

        case S3Operation.DELETE_BUCKET_LIFECYCLE:
            try:
                await target.execute(
                    S3Operation.DELETE_BUCKET_LIFECYCLE,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchLifecycleConfiguration"):
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Lifecycle already absent on target, treating delete as success",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.PUT_BUCKET_POLICY:
            try:
                policy_resp = await source.execute(
                    S3Operation.GET_BUCKET_POLICY,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchBucketPolicy"):
                    _skip_source_gone(
                        "Source bucket or policy no longer exists, skipping replication",
                        e,
                    )
                    return
                raise

            policy = policy_resp.get("Policy")
            if policy:
                await target.execute(
                    S3Operation.PUT_BUCKET_POLICY,
                    {
                        "Bucket": message.bucket,
                        "Policy": policy,
                    },
                )

        case S3Operation.DELETE_BUCKET_POLICY:
            try:
                await target.execute(
                    S3Operation.DELETE_BUCKET_POLICY,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchBucketPolicy"):
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Policy already absent on target, treating delete as success",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case _:
            tracker.record_replication_consumer_outcome(operation.value, target_name, "skipped_unsupported")
            logger.warning(
                "Unsupported replication operation",
                operation=operation.value,
                message_id=message.message_id,
                target=target_name,
            )
