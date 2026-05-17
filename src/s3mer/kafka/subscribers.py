"""Kafka subscriber for replication messages."""

import asyncio
from typing import Any, cast

from aiokafka import ConsumerRecord, TopicPartition
from botocore.exceptions import ClientError
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import KafkaMessage

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.common.errors import ErrorAction, ErrorClassifier
from s3mer.common.logging import get_logger
from s3mer.config.settings import ReplicationMode
from s3mer.kafka.messages import ReplicationMessage
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class ReplicationDelayConfig:
    """Replication delay configuration class to avoid global mutations."""

    retry_delay: float = 1.0
    max_retry_delay: float = 60.0


# Keep strong references to background tasks to prevent garbage collection (RUF006)
_background_tasks: set[asyncio.Task[Any]] = set()


def register_subscribers(
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    mode: ReplicationMode = ReplicationMode.BATCH,
    kafka_config: Any = None,
) -> None:
    """
    Register the replication message subscriber(s) on the broker.

    Depending on the replication mode, registers either:
    1. A single consolidated batch subscriber.
    2. Isolated per-backend subscribers (one per secondary backend).
    """
    if kafka_config is not None:
        ReplicationDelayConfig.retry_delay = kafka_config.replication_retry_delay
        ReplicationDelayConfig.max_retry_delay = kafka_config.replication_max_retry_delay

    if mode == ReplicationMode.PER_BACKEND:
        logger.info("Registering isolated per-backend subscribers")
        for backend in pool.get_secondaries():
            backend_topic = f"{topic}.{backend.name}"
            _register_per_backend_subscriber(broker, backend_topic, pool, backend.name, kafka_config)
    else:
        logger.info("Registering batch subscriber")
        _register_batch_subscriber(broker, topic, pool, kafka_config)


def _register_batch_subscriber(
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    kafka_config: Any = None,
) -> None:
    """
    Register a subscriber for the consolidated 'batch' replication mode.
    Consumes from the base topic and uses Consumer-Level Pausing on failure.
    """
    concurrency = kafka_config.concurrency if kafka_config is not None else 1
    subscriber = broker.subscriber(topic, group_id="s3mer-workers", max_workers=concurrency)

    @subscriber
    async def handle_batch_replication(msg_raw: str, msg: KafkaMessage) -> None:
        message = ReplicationMessage.model_validate_json(msg_raw)

        # Type guard to resolve ConsumerRecord | tuple[ConsumerRecord, ...] union for ty check
        raw = msg.raw_message
        record = cast("ConsumerRecord", raw[0] if isinstance(raw, tuple) else raw)
        partition = record.partition
        offset = record.offset

        failed_tp = TopicPartition(topic, partition)

        logger.info(
            "Processing batch replication message",
            message_id=message.message_id,
            partition=partition,
            offset=offset,
            operation=message.operation,
            targets=message.target_backends,
        )

        operation = S3Operation(message.operation)
        source = pool.get(message.source_backend)

        failed_targets = []
        last_exc: Exception | None = None
        for target_name in message.target_backends:
            target = pool.get(target_name)
            try:
                await _replicate_operation(operation, message, source, target)
                logger.info(
                    "Replication succeeded",
                    message_id=message.message_id,
                    target=target_name,
                )
            except Exception as exc:
                action = ErrorClassifier.classify(exc)
                if action == ErrorAction.FAIL:
                    logger.warning(
                        "Replication failed permanently due to unrecoverable client error. Skipping target.",
                        message_id=message.message_id,
                        target=target_name,
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

        # Trigger Consumer-Level Pause
        consumer = subscriber.consumer
        if consumer:
            assigned = consumer.assignment()
            logger.warning(
                "Batch replication failed. Pausing all assigned partitions to prevent rebalance storm.",
                message_id=message.message_id,
                failed_targets=failed_targets,
                assigned_partitions=[(tp.topic, tp.partition) for tp in assigned],
            )
            consumer.pause(*assigned)

            # Start single global retry background task
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
                )
            )
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)

            raise RuntimeError(f"Replication failed for targets {failed_targets}. Consumer paused.") from last_exc


def _register_per_backend_subscriber(
    broker: KafkaBroker,
    topic: str,
    pool: BackendPool,
    backend_name: str,
    kafka_config: Any = None,
) -> None:
    """
    Register a subscriber for a specific secondary backend.
    Consumes from f"{topic}.{backend_name}" and uses Per-Backend Pausing on failure.
    """
    concurrency = kafka_config.concurrency if kafka_config is not None else 1
    subscriber = broker.subscriber(topic, group_id=f"s3mer-workers-{backend_name}", max_workers=concurrency)

    @subscriber
    async def handle_per_backend_replication(msg_raw: str, msg: KafkaMessage) -> None:
        message = ReplicationMessage.model_validate_json(msg_raw)

        # Type guard to resolve ConsumerRecord | tuple[ConsumerRecord, ...] union for ty check
        raw = msg.raw_message
        record = cast("ConsumerRecord", raw[0] if isinstance(raw, tuple) else raw)
        partition = record.partition
        offset = record.offset

        failed_tp = TopicPartition(topic, partition)

        logger.info(
            "Processing per-backend replication message",
            message_id=message.message_id,
            partition=partition,
            offset=offset,
            operation=message.operation,
            target=backend_name,
        )

        operation = S3Operation(message.operation)
        source = pool.get(message.source_backend)
        target = pool.get(backend_name)

        try:
            await _replicate_operation(operation, message, source, target)
            logger.info(
                "Replication succeeded",
                message_id=message.message_id,
                target=backend_name,
            )
        except Exception as exc:
            action = ErrorClassifier.classify(exc)
            if action == ErrorAction.FAIL:
                logger.warning(
                    "Replication failed permanently due to unrecoverable client error. Skipping.",
                    message_id=message.message_id,
                    target=backend_name,
                    error=str(exc),
                )
                return

            logger.exception(
                "Replication failed",
                message_id=message.message_id,
                target=backend_name,
                error=str(exc),
            )

            # Trigger Per-Backend Pause (only pause this partition)
            consumer = subscriber.consumer
            if consumer:
                logger.warning(
                    "Per-backend replication failed. Pausing partition.",
                    message_id=message.message_id,
                    target=backend_name,
                    partition=partition,
                    offset=offset,
                )
                consumer.pause(failed_tp)

                # Start per-backend background retry task
                task = asyncio.create_task(
                    _schedule_per_backend_retry(
                        subscriber=subscriber,
                        failed_tp=failed_tp,
                        failed_offset=offset,
                        message=message,
                        operation=operation,
                        source=source,
                        target=target,
                    )
                )
                _background_tasks.add(task)
                task.add_done_callback(_background_tasks.discard)

                raise RuntimeError(f"Replication failed for target {backend_name}. Partition paused.") from exc


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
) -> None:
    """Background task to retry batch replication and resume all partitions."""
    attempt = 1
    while True:
        delay = min(
            ReplicationDelayConfig.retry_delay * (2 ** (attempt - 1)),
            ReplicationDelayConfig.max_retry_delay,
        )
        logger.info(
            "Background retry: Waiting to retry replication",
            message_id=message.message_id,
            attempt=attempt,
            delay=delay,
            failed_targets=failed_targets,
        )
        await asyncio.sleep(delay)

        still_failed = []
        for target_name in failed_targets:
            target = pool.get(target_name)
            try:
                await _replicate_operation(operation, message, source, target)
                logger.info(
                    "Background retry: Replication succeeded",
                    message_id=message.message_id,
                    target=target_name,
                    attempt=attempt,
                )
            except Exception as exc:
                action = ErrorClassifier.classify(exc)
                if action == ErrorAction.FAIL:
                    logger.warning(
                        "Background retry: Replication failed permanently due to "
                        "unrecoverable client error. Skipping target.",
                        message_id=message.message_id,
                        target=target_name,
                        error=str(exc),
                    )
                    continue

                logger.exception(
                    "Background retry: Replication failed",
                    message_id=message.message_id,
                    target=target_name,
                    attempt=attempt,
                    error=str(exc),
                )
                still_failed.append(target_name)

        if not still_failed:
            logger.info(
                "Background retry: All replication tasks succeeded. Resuming consumer.",
                message_id=message.message_id,
                partition=failed_tp.partition,
                offset=failed_offset,
            )
            try:
                # Seek the failed partition back to the failed offset
                subscriber.consumer.seek(failed_tp, failed_offset)
                # Resume all assigned partitions
                subscriber.consumer.resume(*assigned_partitions)
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
) -> None:
    """Background task to retry per-backend replication and resume the partition."""
    attempt = 1
    while True:
        delay = min(
            ReplicationDelayConfig.retry_delay * (2 ** (attempt - 1)),
            ReplicationDelayConfig.max_retry_delay,
        )
        logger.info(
            "Background retry: Waiting to retry replication",
            message_id=message.message_id,
            target=target.name,
            attempt=attempt,
            delay=delay,
        )
        await asyncio.sleep(delay)

        try:
            await _replicate_operation(operation, message, source, target)
            logger.info(
                "Background retry: Replication succeeded. Resuming partition.",
                message_id=message.message_id,
                target=target.name,
                attempt=attempt,
            )
            # Seek consumer back to failed offset
            subscriber.consumer.seek(failed_tp, failed_offset)
            # Resume only this partition
            subscriber.consumer.resume(failed_tp)
            break
        except Exception as exc:
            action = ErrorClassifier.classify(exc)
            if action == ErrorAction.FAIL:
                logger.warning(
                    "Background retry: Replication failed permanently due to "
                    "unrecoverable client error. Skipping and resuming partition.",
                    message_id=message.message_id,
                    target=target.name,
                    error=str(exc),
                )
                try:
                    # Seek past the failed message and resume partition
                    subscriber.consumer.seek(failed_tp, failed_offset + 1)
                    subscriber.consumer.resume(failed_tp)
                except Exception as e:
                    logger.exception("Failed to resume partition. Will retry in next loop.", error=str(e))
                    attempt += 1
                    continue
                break

            logger.exception(
                "Background retry: Replication failed",
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
) -> None:
    """
    Replicate a single S3 operation from source to target backend.

    For PutObject, reads the object from the source and writes it to the target.
    For other operations, replays the operation directly on the target.
    """
    match operation:
        case S3Operation.PUT_OBJECT:
            # Read from source, stream to target
            try:
                get_response = await source.execute(
                    S3Operation.GET_OBJECT,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchKey", "NoSuchBucket"):
                    logger.warning(
                        "Source object or bucket no longer exists, skipping replication",
                        bucket=message.bucket,
                        key=message.key,
                        error=str(e),
                    )
                    return
                raise

            # We pass the StreamingBody directly to the target execute call.
            # aiobotocore handles the underlying async stream.
            body = get_response["Body"]
            put_params: dict = {
                "Bucket": message.bucket,
                "Key": message.key,
                "Body": body,
            }

            # Preserve metadata from the message
            if "ContentType" in message.metadata:
                put_params["ContentType"] = message.metadata["ContentType"]

            # ContentLength is mandatory for streaming PutObject.
            # If it's missing from the message (e.g. multipart), fetch it from source.
            content_length = message.metadata.get("ContentLength")
            if content_length is None:
                # HEAD object on source to get exact size
                try:
                    head_resp = await source.execute(
                        S3Operation.HEAD_OBJECT,
                        {"Bucket": message.bucket, "Key": message.key},
                    )
                    content_length = head_resp["ContentLength"]
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code in ("NoSuchKey", "NoSuchBucket"):
                        logger.warning(
                            "Source object or bucket no longer exists on HEAD, skipping replication",
                            bucket=message.bucket,
                            key=message.key,
                            error=str(e),
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
            await target.execute(
                S3Operation.DELETE_OBJECT,
                {"Bucket": message.bucket, "Key": message.key},
            )

        case S3Operation.PUT_OBJECT_TAGGING:
            # Fetch current tagging from source and apply to target
            try:
                tag_response = await source.execute(
                    S3Operation.GET_OBJECT_TAGGING,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchKey", "NoSuchBucket"):
                    logger.warning(
                        "Source object or bucket no longer exists for tagging, skipping replication",
                        bucket=message.bucket,
                        key=message.key,
                        error=str(e),
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
            await target.execute(
                S3Operation.DELETE_OBJECT_TAGGING,
                {"Bucket": message.bucket, "Key": message.key},
            )

        case S3Operation.PUT_BUCKET_LIFECYCLE:
            # Fetch current lifecycle configuration from source and apply to target
            try:
                lifecycle_resp = await source.execute(
                    S3Operation.GET_BUCKET_LIFECYCLE,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchLifecycleConfiguration"):
                    logger.warning(
                        "Source bucket or lifecycle configuration no longer exists, skipping replication",
                        bucket=message.bucket,
                        error=str(e),
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
                    logger.warning(
                        "Target bucket or lifecycle configuration no longer exists, skipping delete replication",
                        bucket=message.bucket,
                        error=str(e),
                    )
                    return
                raise

        case S3Operation.PUT_BUCKET_POLICY:
            # Fetch current policy from source and apply to target
            try:
                policy_resp = await source.execute(
                    S3Operation.GET_BUCKET_POLICY,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchBucketPolicy"):
                    logger.warning(
                        "Source bucket or policy no longer exists, skipping replication",
                        bucket=message.bucket,
                        error=str(e),
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
                    logger.warning(
                        "Target bucket or policy no longer exists, skipping delete replication",
                        bucket=message.bucket,
                        error=str(e),
                    )
                    return
                raise

        case _:
            logger.warning(
                "Unsupported replication operation",
                operation=operation.value,
                message_id=message.message_id,
            )
