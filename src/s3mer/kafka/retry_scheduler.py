"""Background retry scheduling for replication consumers."""

import asyncio
from typing import Any

from aiokafka import TopicPartition

from s3mer.backends.pool import BackendPool
from s3mer.backends.types import BackendClient
from s3mer.common.errors import ErrorAction, ErrorClassifier
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.kafka.dlq import DlqPublisher
from s3mer.kafka.message_validation import record_skip, resume_partitions
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.replication_executor import replicate_operation
from s3mer.kafka.subscribers_config import ReplicationRetryConfig
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)

_background_tasks: set[asyncio.Task[Any]] = set()
_background_retries_in_flight: dict[str, int] = {"batch": 0, "per_backend": 0}


def track_background_task(task: asyncio.Task[Any]) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def retry_delay_seconds(attempt: int) -> float:
    return min(
        ReplicationRetryConfig.retry_delay * (2 ** (attempt - 1)),
        ReplicationRetryConfig.max_retry_delay,
    )


def _inc_background_retries(mode: str, metrics: MetricsTracker) -> None:
    _background_retries_in_flight[mode] = _background_retries_in_flight.get(mode, 0) + 1
    metrics.set_replication_background_retries_in_flight(mode, _background_retries_in_flight[mode])


def _dec_background_retries(mode: str, metrics: MetricsTracker) -> None:
    _background_retries_in_flight[mode] = max(0, _background_retries_in_flight.get(mode, 0) - 1)
    metrics.set_replication_background_retries_in_flight(mode, _background_retries_in_flight[mode])


def advance_consumer_offset(
    subscriber: Any,
    failed_tp: TopicPartition,
    failed_offset: int,
    metrics: MetricsTracker,
    *,
    partitions_to_resume: set[TopicPartition] | None = None,
) -> None:
    consumer = subscriber.consumer
    if consumer is None:
        msg = "Cannot advance consumer offset: consumer is not available"
        raise RuntimeError(msg)
    consumer.seek(failed_tp, failed_offset + 1)
    if partitions_to_resume:
        resume_partitions(consumer, metrics, *partitions_to_resume)
    else:
        resume_partitions(consumer, metrics, failed_tp)


async def schedule_global_retry(
    subscriber: Any,
    failed_tp: TopicPartition,
    failed_offset: int,
    assigned_partitions: set[TopicPartition],
    message: ReplicationMessage,
    operation: S3Operation,
    source: BackendClient,
    failed_targets: list[str],
    pool: BackendPool,
    metrics: MetricsTracker,
    dlq: DlqPublisher | None = None,
) -> None:
    """Background task to retry batch replication and resume all partitions."""
    _inc_background_retries("batch", metrics)
    try:
        attempt = 1
        while True:
            if attempt > ReplicationRetryConfig.max_retries:
                for target_name in failed_targets:
                    await record_skip(
                        metrics,
                        message.operation,
                        target_name,
                        "skipped_max_retries",
                        message_id=message.message_id,
                        partition=failed_tp.partition,
                        offset=failed_offset,
                        message=message,
                        dlq=dlq,
                        per_backend=False,
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
                    advance_consumer_offset(
                        subscriber,
                        failed_tp,
                        failed_offset,
                        metrics,
                        partitions_to_resume=assigned_partitions,
                    )
                except Exception as e:
                    logger.exception(
                        "Failed to advance consumer after max retries. Will retry advance in next loop.",
                        error=str(e),
                    )
                    attempt += 1
                    continue
                break

            delay = retry_delay_seconds(attempt)
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
                    await replicate_operation(operation, message, source, target, metrics)
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
                        await record_skip(
                            metrics,
                            message.operation,
                            target_name,
                            "skipped_permanent",
                            message_id=message.message_id,
                            partition=failed_tp.partition,
                            offset=failed_offset,
                            error=str(exc),
                            message=message,
                            dlq=dlq,
                            per_backend=False,
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
                    resume_partitions(subscriber.consumer, metrics, *assigned_partitions)
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
    finally:
        _dec_background_retries("batch", metrics)


async def schedule_per_backend_retry(
    subscriber: Any,
    failed_tp: TopicPartition,
    failed_offset: int,
    message: ReplicationMessage,
    operation: S3Operation,
    source: BackendClient,
    target: BackendClient,
    metrics: MetricsTracker,
    dlq: DlqPublisher | None = None,
) -> None:
    """Background task to retry per-backend replication and resume the partition."""
    _inc_background_retries("per_backend", metrics)
    try:
        attempt = 1
        while True:
            if attempt > ReplicationRetryConfig.max_retries:
                await record_skip(
                    metrics,
                    message.operation,
                    target.name,
                    "skipped_max_retries",
                    message_id=message.message_id,
                    partition=failed_tp.partition,
                    offset=failed_offset,
                    message=message,
                    dlq=dlq,
                    per_backend=True,
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
                    advance_consumer_offset(subscriber, failed_tp, failed_offset, metrics)
                except Exception as e:
                    logger.exception(
                        "Failed to advance consumer after max retries. Will retry advance in next loop.",
                        error=str(e),
                    )
                    attempt += 1
                    continue
                break

            delay = retry_delay_seconds(attempt)
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
                await replicate_operation(operation, message, source, target, metrics)
                metrics.record_replication_consumer_outcome(message.operation, target.name, "success")
                logger.info(
                    "Background retry: replication succeeded. Resuming partition.",
                    message_id=message.message_id,
                    target=target.name,
                    attempt=attempt,
                )
                subscriber.consumer.seek(failed_tp, failed_offset)
                resume_partitions(subscriber.consumer, metrics, failed_tp)
                break
            except Exception as exc:
                action = ErrorClassifier.classify(exc)
                if action == ErrorAction.FAIL:
                    await record_skip(
                        metrics,
                        message.operation,
                        target.name,
                        "skipped_permanent",
                        message_id=message.message_id,
                        partition=failed_tp.partition,
                        offset=failed_offset,
                        error=str(exc),
                        message=message,
                        dlq=dlq,
                        per_backend=True,
                    )
                    try:
                        advance_consumer_offset(subscriber, failed_tp, failed_offset, metrics)
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
    finally:
        _dec_background_retries("per_backend", metrics)
