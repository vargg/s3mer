"""Abstract strategy protocol for handling S3 operations."""

from typing import Any, Protocol

from s3mer.backends.pool import BackendPool
from s3mer.routing.operations import S3Operation


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
