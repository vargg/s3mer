"""Manager for handling replication logic across backends."""

from typing import Any

from s3m.common.logging import get_logger
from s3m.kafka.messages import ReplicationMessage
from s3m.kafka.publisher import ReplicationPublisher
from s3m.routing.operations import S3Operation

logger = get_logger(__name__)


class ReplicationManager:
    """
    Handles the logic of how to propagate operations across backends.

    This class decouples the 'Propagation' concern from the 'Execution' concern.
    It decides what replication messages to send based on the operation performed.
    """

    def __init__(self, publisher: ReplicationPublisher) -> None:
        self._publisher = publisher

    @property
    def publisher(self) -> ReplicationPublisher:
        """Get the underlying replication publisher."""
        return self._publisher

    async def schedule_replication(
        self,
        operation: S3Operation,
        params: dict[str, Any],
        response: dict[str, Any],
        source_backend_name: str,
        target_backend_names: list[str],
    ) -> None:
        """
        Determine the appropriate replication messages and publish them to Kafka.
        """
        if not target_backend_names:
            return

        # Build metadata from both params and response
        metadata: dict[str, Any] = {}
        for key in ("ETag", "ContentType", "ContentLength"):
            if key in response:
                metadata[key] = response[key]
            elif key in params:
                metadata[key] = params[key]

        # Determine the replication operation
        # If we're completing a multipart upload or copying an object,
        # the replication operation should actually be PUT_OBJECT
        # so the worker can just read the fully assembled/copied object.
        rep_op = (
            S3Operation.PUT_OBJECT.value
            if operation in (S3Operation.COMPLETE_MULTIPART_UPLOAD, S3Operation.COPY_OBJECT)
            else operation.value
        )

        if operation == S3Operation.DELETE_OBJECTS:
            # Fan-out: replicate each deleted object individually
            objects = params.get("Delete", {}).get("Objects", [])
            for obj in objects:
                message = ReplicationMessage(
                    operation=S3Operation.DELETE_OBJECT.value,
                    bucket=params.get("Bucket", ""),
                    key=obj["Key"],
                    source_backend=source_backend_name,
                    target_backends=target_backend_names,
                    metadata=metadata,
                )
                await self._publisher.publish(message)
        else:
            # Standard single message replication
            message = ReplicationMessage(
                operation=rep_op,
                bucket=params.get("Bucket", ""),
                key=params.get("Key"),
                source_backend=source_backend_name,
                target_backends=target_backend_names,
                metadata=metadata,
            )
            await self._publisher.publish(message)

        logger.info(
            "Replication scheduled",
            operation=operation.value,
            source=source_backend_name,
            targets=target_backend_names,
            num_messages=len(params.get("Delete", {}).get("Objects", []))
            if operation == S3Operation.DELETE_OBJECTS
            else 1,
        )
