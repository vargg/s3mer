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

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute a write operation: primary sync + Kafka replication.

        Args:
            operation: The write operation (PutObject, CreateBucket, etc.).
            pool: Backend pool.
            params: Boto3 method parameters (including Body for PutObject).

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

            message = ReplicationMessage(
                operation=operation.value,
                bucket=params.get("Bucket", ""),
                key=params.get("Key"),
                source_backend=primary.name,
                target_backends=[b.name for b in secondaries],
                metadata=metadata,
            )
            await self._publisher.publish(message)
            logger.info(
                "Replication message published",
                operation=operation.value,
                targets=[b.name for b in secondaries],
                message_id=message.message_id,
            )

        return response
