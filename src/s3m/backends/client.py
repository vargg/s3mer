"""S3 backend client wrapping aiobotocore for a single storage backend."""

from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session

from s3m.common.logging import get_logger
from s3m.config.settings import BackendConfig
from s3m.routing.operations import S3Operation

logger = get_logger(__name__)


class S3BackendClient:
    """
    Async S3 client for a single backend.

    The client is initialized once at startup and reused across requests.
    Wraps aiobotocore to provide a consistent interface for executing
    S3 operations against a specific backend.
    """

    def __init__(self, config: BackendConfig) -> None:
        self.config = config
        self.name = config.name
        self.is_primary = config.is_primary
        self.priority = config.priority
        self._client: Any = None
        self._session = get_session()

    async def start(self) -> None:
        """Initialize the aiobotocore client. Call once at app startup."""
        self._client = await self._session.create_client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            region_name=self.config.region,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key.get_secret_value(),
            config=AioConfig(
                s3={"addressing_style": self.config.addressing_style, "payload_signing_enabled": False},
                request_checksum_calculation="when_required",
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 2},
            ),
        ).__aenter__()
        logger.info("Backend client started", backend=self.name, endpoint=self.config.endpoint_url)

    async def close(self) -> None:
        """Close the aiobotocore client. Call once at app shutdown."""
        if self._client:
            await self._client.__aexit__(None, None, None)
            self._client = None
            logger.info("Backend client closed", backend=self.name)

    async def execute(self, operation: S3Operation, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute an S3 operation on this backend.

        Args:
            operation: The S3 operation to execute.
            params: Boto3 method kwargs (Bucket, Key, Body, etc.).

        Returns:
            The raw boto3 response dict.

        Raises:
            botocore.exceptions.ClientError: On S3 errors.
            ConnectionError: If the backend is unreachable.
        """
        if self._client is None:
            raise RuntimeError(f"Backend client {self.name} not started — call start() first")

        method = getattr(self._client, operation.boto_method)

        logger.debug(
            "Executing S3 operation",
            backend=self.name,
            operation=operation.value,
            bucket=params.get("Bucket"),
            key=params.get("Key"),
        )

        return await method(**params)
