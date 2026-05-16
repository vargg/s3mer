"""Kafka subscriber for replication messages."""

import asyncio

from faststream.kafka import KafkaBroker

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.common.logging import get_logger
from s3mer.kafka.messages import ReplicationMessage
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 1


def register_subscribers(broker: KafkaBroker, topic: str, pool: BackendPool) -> None:
    """
    Register the replication message subscriber on the broker.

    This is called during worker startup to wire up the message handler.
    """

    @broker.subscriber(topic, group_id="s3mer-workers")
    async def handle_replication(msg_raw: str) -> None:
        """
        Process a replication message — replicate an S3 operation
        from the source backend to all target backends.
        """
        message = ReplicationMessage.model_validate_json(msg_raw)

        logger.info(
            "Processing replication message",
            message_id=message.message_id,
            operation=message.operation,
            bucket=message.bucket,
            key=message.key,
            source=message.source_backend,
            targets=message.target_backends,
        )

        operation = S3Operation(message.operation)
        source = pool.get(message.source_backend)

        failed_targets = []
        for target_name in message.target_backends:
            target = pool.get(target_name)
            try:
                await _replicate_operation(operation, message, source, target)
                logger.info(
                    "Replication succeeded",
                    message_id=message.message_id,
                    operation=message.operation,
                    target=target_name,
                )
            except Exception as exc:
                logger.exception(
                    "Replication failed",
                    message_id=message.message_id,
                    operation=message.operation,
                    target=target_name,
                    error=str(exc),
                    retry_count=message.retry_count,
                )
                failed_targets.append(target_name)

        if not failed_targets:
            return

        message.retry_count += 1
        message.target_backends = failed_targets

        if message.retry_count > MAX_RETRIES:
            logger.error(
                "Max retries exceeded, routing to DLQ",
                message_id=message.message_id,
                operation=message.operation,
                targets=failed_targets,
            )
            await broker.publish(message.model_dump_json(), topic="s3mer.replication.dlq")
        else:
            await asyncio.sleep(RETRY_DELAY)
            await broker.publish(message.model_dump_json(), topic=topic)


async def _replicate_operation(
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
            get_response = await source.execute(
                S3Operation.GET_OBJECT,
                {"Bucket": message.bucket, "Key": message.key},
            )

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
                head_resp = await source.execute(
                    S3Operation.HEAD_OBJECT,
                    {"Bucket": message.bucket, "Key": message.key},
                )
                content_length = head_resp["ContentLength"]

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
            tag_response = await source.execute(
                S3Operation.GET_OBJECT_TAGGING,
                {"Bucket": message.bucket, "Key": message.key},
            )
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

        case _:
            logger.warning(
                "Unsupported replication operation",
                operation=operation.value,
                message_id=message.message_id,
            )
