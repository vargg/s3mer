"""Write strategy — write to primary, replicate to secondaries via Kafka."""

from typing import Any

from s3m.backends.pool import BackendPool
from s3m.common.logging import get_logger
from s3m.kafka.messages import ReplicationMessage
from s3m.kafka.publisher import ReplicationPublisher
from s3m.routing.operations import S3Operation

logger = get_logger(__name__)


class WritePrimaryReplicationStrategy:
    """
    Write strategy that writes to the primary backend synchronously,
    then publishes a replication message to Kafka for async replication
    to secondary backends.
    """

    def __init__(self, publisher: ReplicationPublisher) -> None:
        self._publisher = publisher

    @property
    def publisher(self) -> ReplicationPublisher:
        """Get the replication publisher."""
        return self._publisher

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        """
        Execute a write operation: primary sync + optional Kafka replication.

        Args:
            operation: The write operation (PutObject, CreateBucket, etc.).
            pool: Backend pool.
            params: Boto3 method parameters (including Body for PutObject).
            replicate: Whether to publish a replication message.

        Returns:
            The primary backend's response dict.

        Raises:
            botocore.exceptions.ClientError: If the primary write fails.
        """
        primary = pool.primary

        # 1. Synchronous write to primary
        response = await primary.execute(operation, params)
        logger.info(
            "Write operation succeeded on primary",
            operation=operation.value,
            backend=primary.name,
            bucket=params.get("Bucket"),
            key=params.get("Key"),
        )

        if not replicate:
            return response

        # 2. Publish replication message for secondary backends
        secondaries = pool.get_secondaries()
        if secondaries:
            # Build metadata from the primary's response
            metadata: dict[str, Any] = {}
            if "ETag" in response:
                metadata["ETag"] = response["ETag"]
            if "ContentType" in params:
                metadata["ContentType"] = params["ContentType"]
            if "ContentLength" in params:
                metadata["ContentLength"] = params["ContentLength"]

            # If we're completing a multipart upload or copying an object,
            # the replication operation should actually be PUT_OBJECT
            # so the worker can just read the fully assembled/copied object.
            rep_op = (
                S3Operation.PUT_OBJECT.value
                if operation in (S3Operation.COMPLETE_MULTIPART_UPLOAD, S3Operation.COPY_OBJECT)
                else operation.value
            )

            message = ReplicationMessage(
                operation=rep_op,
                bucket=params.get("Bucket", ""),
                key=params.get("Key"),
                source_backend=primary.name,
                target_backends=[b.name for b in secondaries],
                metadata=metadata,
            )
            await self._publisher.publish(message)
            logger.info(
                "Replication message published",
                operation=rep_op,
                targets=[b.name for b in secondaries],
                message_id=message.message_id,
            )

        return response

        return response
