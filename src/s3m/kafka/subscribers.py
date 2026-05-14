"""Kafka subscriber for replication messages."""

from __future__ import annotations

from faststream.kafka import KafkaBroker

from s3m.backends.pool import BackendPool
from s3m.common.logging import get_logger
from s3m.kafka.messages import ReplicationMessage
from s3m.routing.operations import S3Operation

logger = get_logger(__name__)


def register_subscribers(broker: KafkaBroker, topic: str, pool: BackendPool) -> None:
    """
    Register the replication message subscriber on the broker.

    This is called during worker startup to wire up the message handler.
    """

    @broker.subscriber(topic, group_id="s3m-workers")
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
                logger.error(
                    "Replication failed",
                    message_id=message.message_id,
                    operation=message.operation,
                    target=target_name,
                    error=str(exc),
                    retry_count=message.retry_count,
                )
                # TODO: implement DLQ / retry logic


async def _replicate_operation(
    operation: S3Operation,
    message: ReplicationMessage,
    source: object,
    target: object,
) -> None:
    """
    Replicate a single S3 operation from source to target backend.

    For PutObject, reads the object from the source and writes it to the target.
    For other operations, replays the operation directly on the target.
    """
    from s3m.backends.client import S3BackendClient

    assert isinstance(source, S3BackendClient)
    assert isinstance(target, S3BackendClient)

    match operation:
        case S3Operation.PUT_OBJECT:
            # Read from source, write to target
            get_response = await source.execute(
                S3Operation.GET_OBJECT,
                {"Bucket": message.bucket, "Key": message.key},
            )

            # Read the full body for replication
            body_chunks: list[bytes] = []
            async with get_response["Body"] as stream:
                while True:
                    chunk = await stream.read(65_536)
                    if not chunk:
                        break
                    body_chunks.append(chunk)
            body = b"".join(body_chunks)

            put_params: dict = {
                "Bucket": message.bucket,
                "Key": message.key,
                "Body": body,
            }
            if "ContentType" in message.metadata:
                put_params["ContentType"] = message.metadata["ContentType"]

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

        case _:
            logger.warning(
                "Unsupported replication operation",
                operation=operation.value,
                message_id=message.message_id,
            )
