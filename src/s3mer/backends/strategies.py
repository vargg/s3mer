"""S3 Operation & Replication Strategies."""

from collections.abc import AsyncIterator
from typing import Any, Protocol

from s3mer.backends.pool import BackendPool
from s3mer.common.errors import ErrorAction, ErrorClassifier
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.common.streaming import BufferedStreamReader
from s3mer.kafka.manager import BaseReplicationManager
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class OperationStrategy(Protocol):
    """Protocol for S3 operation execution strategies."""

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
    ) -> Any:
        """
        Execute an S3 operation using the configured strategy.

        Args:
            operation: The S3 operation to execute.
            pool: The backend pool to use.
            params: Boto3 method parameters.

        Returns:
            The operation result (response dict or streaming body).
        """
        ...


class ReadFallbackStrategy:
    """
    Read strategy that iterates backends by priority.

    Tries each backend in priority order (lowest first).
    Returns the first successful response.
    If all backends fail, raises the last error.
    """

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute a read operation with fallback across backends.

        Args:
            operation: The read operation (GetObject, HeadObject, etc.).
            pool: Backend pool sorted by priority.
            params: Boto3 method parameters.

        Returns:
            The first successful response dict.

        Raises:
            The last exception if all backends fail.
        """
        backends = pool.all_by_priority()
        last_error: Exception | None = None

        for backend in backends:
            try:
                response = await backend.execute(operation, params)
                logger.info(
                    "Read operation succeeded",
                    operation=operation.value,
                    backend=backend.name,
                    bucket=params.get("Bucket"),
                    key=params.get("Key"),
                )
            except Exception as exc:
                logger.warning(
                    "Read operation failed on backend, trying next",
                    operation=operation.value,
                    backend=backend.name,
                    error=str(exc),
                )
                last_error = exc
            else:
                return response

        # All backends failed
        logger.error(
            "Read operation failed on all backends",
            operation=operation.value,
            bucket=params.get("Bucket"),
            key=params.get("Key"),
        )
        if last_error is None:
            raise RuntimeError("No backends configured")
        raise last_error


class WritePrimaryReplicationStrategy:
    """
    Write strategy that writes to the primary backend synchronously,
    then publishes a replication message to Kafka for async replication
    to secondary backends.
    """

    def __init__(self, replication_manager: BaseReplicationManager, metrics: MetricsTracker) -> None:
        self._replication_manager = replication_manager
        self._metrics = metrics

    @property
    def publisher(self) -> ReplicationPublisher:
        """Get the underlying replication publisher."""
        return self._replication_manager.publisher

    async def execute(
        self,
        operation: S3Operation,
        pool: BackendPool,
        params: dict[str, Any],
        *,
        replicate: bool = True,
    ) -> dict[str, Any]:
        """
        Execute a write operation with fallback support.

        Tries backends in order (Primary first). If a backend fails with a
        retryable error, attempts the next available backend.
        """
        candidates = pool.get_write_candidates()

        # If Body is an AsyncIterator, wrap it to allow replaying on fallback
        original_body = params.get("Body")
        if original_body and isinstance(original_body, AsyncIterator):
            params["Body"] = BufferedStreamReader(original_body, self._metrics)

        response: dict[str, Any] | None = None
        successful_backend = None
        last_error = None

        for i, backend in enumerate(candidates):
            try:
                # If this is a fallback attempt, rewind the buffered body
                if i > 0 and isinstance(params.get("Body"), BufferedStreamReader):
                    params["Body"].seek_to_start()

                response = await backend.execute(operation, params)
                successful_backend = backend
                logger.info(
                    "Write operation succeeded",
                    backend=backend.name,
                    operation=operation.value,
                    attempt=i + 1,
                )
                break
            except Exception as e:
                last_error = e
                action = ErrorClassifier.classify(e)
                if action == ErrorAction.FAIL:
                    logger.warning(
                        "Client/permanent error on write, failing immediately without fallback",
                        backend=backend.name,
                        operation=operation.value,
                        error=str(e),
                    )
                    raise
                logger.warning(
                    "Error on write, trying fallback",
                    backend=backend.name,
                    operation=operation.value,
                    action=action.value,
                    error=str(e),
                )

        if successful_backend is None or response is None:
            if last_error:
                raise last_error
            raise RuntimeError("No backends succeeded for write operation")

        # Clean up buffer if we used one
        if isinstance(params.get("Body"), BufferedStreamReader):
            params["Body"].close()

        if replicate:
            # 2. Replicate to ALL OTHER backends
            targets = [b for b in pool.all_clients if b.name != successful_backend.name]
            if targets:
                await self._replication_manager.schedule_replication(
                    operation=operation,
                    params=params,
                    response=response,
                    source_backend_name=successful_backend.name,
                    target_backend_names=[b.name for b in targets],
                )

        return response
