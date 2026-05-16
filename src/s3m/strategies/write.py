"""Write strategy — write to primary, replicate to secondaries via Kafka."""

from collections.abc import AsyncIterator
from http import HTTPStatus
from typing import Any

from botocore.exceptions import ClientError

from s3m.backends.pool import BackendPool
from s3m.common.logging import get_logger
from s3m.common.streaming import BufferedStreamReader
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

    async def execute(  # noqa: PLR0912
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
            params["Body"] = BufferedStreamReader(original_body)

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
            except ClientError as e:
                # Don't fallback on 4xx errors (client errors), only 5xx or connection issues
                status_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
                if HTTPStatus.BAD_GATEWAY <= status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
                    logger.warning("Client error on write, not retrying", backend=backend.name, error=str(e))
                    raise
                last_error = e
                logger.warning("Server error on write, trying fallback", backend=backend.name, error=str(e))
            except Exception as e:
                last_error = e
                logger.warning("Unexpected error on write, trying fallback", backend=backend.name, error=str(e))

        if successful_backend is None or response is None:
            if last_error:
                raise last_error
            raise RuntimeError("No backends succeeded for write operation")

        # Clean up buffer if we used one
        if isinstance(params.get("Body"), BufferedStreamReader):
            params["Body"].close()

        if not replicate:
            return response

        # 2. Publish replication message for ALL OTHER backends
        # If we wrote to a secondary, we MUST replicate back to primary.
        targets = [b for b in pool.all_clients if b.name != successful_backend.name]

        if targets:
            # Build metadata from both params and response
            metadata: dict[str, Any] = {}
            for key in ("ETag", "ContentType", "ContentLength"):
                if key in response:
                    metadata[key] = response[key]
                elif key in params:
                    metadata[key] = params[key]

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
                source_backend=successful_backend.name,
                target_backends=[b.name for b in targets],
                metadata=metadata,
            )
            await self._publisher.publish(message)
            logger.info(
                "Replication message published for fallback",
                source=successful_backend.name,
                targets=[b.name for b in targets],
                message_id=message.message_id,
            )

        return response
