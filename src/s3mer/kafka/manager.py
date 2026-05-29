"""Manager for handling replication logic across backends."""

from abc import ABC, abstractmethod
from typing import Any

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class BaseReplicationManager(ABC):
    """
    Base class for replication managers. Decouples the 'Propagation' concern
    from the 'Execution' concern.
    """

    def __init__(self, publisher: ReplicationPublisher, metrics: MetricsTracker) -> None:
        self._publisher = publisher
        self._metrics = metrics

    @property
    def publisher(self) -> ReplicationPublisher:
        """Get the underlying replication publisher."""
        return self._publisher

    @abstractmethod
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

    def _map_operation(self, operation: S3Operation) -> str:
        """Map S3 proxy operation to a replication task operation."""
        if operation in (S3Operation.COMPLETE_MULTIPART_UPLOAD, S3Operation.COPY_OBJECT):
            # Worker should just perform a regular PUT by reading from the source
            return S3Operation.PUT_OBJECT.value

        if operation == S3Operation.DELETE_OBJECTS:
            return S3Operation.DELETE_OBJECT.value

        return operation.value

    def _extract_keys(
        self, operation: S3Operation, params: dict[str, Any], response: dict[str, Any]
    ) -> list[str | None]:
        """Extract all keys that need to be replicated."""
        if operation == S3Operation.DELETE_OBJECTS:
            # We use the response if available (successful deletions), fallback to params
            deleted = response.get("Deleted", [])
            if deleted:
                return [d["Key"] for d in deleted]
            return [obj["Key"] for obj in params.get("Delete", {}).get("Objects", [])]

        # Bucket operations don't have a key, but still need one replication task
        if operation in (S3Operation.CREATE_BUCKET, S3Operation.DELETE_BUCKET):
            return [None]

        key = params.get("Key")
        return [key] if key else []

    def _build_metadata(self, params: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
        """Extract metadata for replication tasks."""
        metadata: dict[str, Any] = {}
        for key in ("ETag", "ContentType", "ContentLength"):
            if key in response:
                metadata[key] = response[key]
            elif key in params:
                metadata[key] = params[key]
        return metadata


class BatchReplicationManager(BaseReplicationManager):
    """
    Handles the logic of how to propagate operations across backends by grouping
    all target backends into a single replication message.
    """

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

        target_operation = self._map_operation(operation)
        keys_to_replicate = self._extract_keys(operation, params, response)

        self._metrics.record_replication_fanout(operation.value, len(keys_to_replicate))

        metadata = self._build_metadata(params, response)

        logger.debug(
            "Scheduling batch replication",
            operation=operation.value,
            target_op=target_operation,
            keys=keys_to_replicate,
            targets=target_backend_names,
        )

        for key in keys_to_replicate:
            msg = ReplicationMessage(
                operation=target_operation,
                bucket=params["Bucket"],
                key=key,
                source_backend=source_backend_name,
                target_backends=target_backend_names,
                metadata=metadata,
            )

            for target in target_backend_names:
                self._metrics.record_replication_task(target_operation, target)

            await self._publisher.publish(msg)

        logger.info(
            "Batch replication scheduled",
            operation=operation.value,
            source=source_backend_name,
            targets=target_backend_names,
            num_tasks=len(keys_to_replicate),
        )


class PerBackendReplicationManager(BaseReplicationManager):
    """
    Publishes a separate ReplicationMessage to Kafka for each individual target backend.
    This allows horizontal scaling and isolates failures per backend at the expense of read amplification.
    """

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

        target_operation = self._map_operation(operation)
        keys_to_replicate = self._extract_keys(operation, params, response)

        self._metrics.record_replication_fanout(operation.value, len(keys_to_replicate))

        metadata = self._build_metadata(params, response)

        logger.debug(
            "Scheduling per-backend replication",
            operation=operation.value,
            target_op=target_operation,
            keys=keys_to_replicate,
            targets=target_backend_names,
        )

        for key in keys_to_replicate:
            for target in target_backend_names:
                msg = ReplicationMessage(
                    operation=target_operation,
                    bucket=params["Bucket"],
                    key=key,
                    source_backend=source_backend_name,
                    target_backends=[target],
                    metadata=metadata,
                )

                self._metrics.record_replication_task(target_operation, target)
                await self._publisher.publish(msg, topic=f"{self._publisher.topic}.{target}")

        logger.info(
            "Per-backend replication scheduled",
            operation=operation.value,
            source=source_backend_name,
            targets=target_backend_names,
            num_tasks=len(keys_to_replicate) * len(target_backend_names),
        )
