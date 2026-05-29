import time
from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.config.settings import BackendConfig
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


class S3BackendClient:
    """
    Async S3 client for a single backend.

    The client is initialized once at startup and reused across requests.
    Wraps aiobotocore to provide a consistent interface for executing
    S3 operations against a specific backend.
    """

    def __init__(self, name: str, config: BackendConfig, metrics: MetricsTracker) -> None:
        self.name = name
        self.config = config
        self.is_primary = config.is_primary
        self.priority = config.priority
        self._metrics = metrics
        self._client: Any = None
        self._session = get_session()
        self.last_latency: float = 0.0

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
                max_pool_connections=self.config.max_pool_connections,
                connect_timeout=self.config.connect_timeout,
                read_timeout=self.config.read_timeout,
                retries={"max_attempts": self.config.max_attempts},
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

        start_time = time.perf_counter()
        try:
            result = await method(**params)
        except Exception:
            duration = time.perf_counter() - start_time
            self._metrics.record_backend_request(self.name, operation.value, "error", duration)
            self._metrics.record_backend_status(self.name, False)
            raise
        else:
            duration = time.perf_counter() - start_time
            self._metrics.record_backend_request(self.name, operation.value, "success", duration)
            self._metrics.record_backend_status(self.name, True)
            return result
