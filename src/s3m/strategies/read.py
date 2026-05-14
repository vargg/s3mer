"""Read fallback strategy — iterate backends, return first success."""

from __future__ import annotations

from typing import Any

from s3m.backends.pool import BackendPool
from s3m.common.logging import get_logger
from s3m.routing.operations import S3Operation

logger = get_logger(__name__)


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
                return response
            except Exception as exc:
                logger.warning(
                    "Read operation failed on backend, trying next",
                    operation=operation.value,
                    backend=backend.name,
                    error=str(exc),
                )
                last_error = exc

        # All backends failed
        logger.error(
            "Read operation failed on all backends",
            operation=operation.value,
            bucket=params.get("Bucket"),
            key=params.get("Key"),
        )
        assert last_error is not None  # at least one backend must exist
        raise last_error
