import time
from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session

from s3mer.backends.circuit_breaker import BackendCircuitBreaker
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
        self.is_primary = config.is_primary
        self.priority = config.priority
        self.last_latency: float = 0.0

        self._config = config
        self._metrics = metrics
        self._client: Any = None
        self._session = get_session()
        self._circuit_breaker: BackendCircuitBreaker | None = None

    def set_circuit_breaker(self, breaker: BackendCircuitBreaker) -> None:
        self._circuit_breaker = breaker

    async def start(self) -> None:
        """Initialize the aiobotocore client. Call once at app startup."""
        self._client = await self._session.create_client(
            "s3",
            endpoint_url=self._config.endpoint_url,
            region_name=self._config.region,
            aws_access_key_id=self._config.access_key,
            aws_secret_access_key=self._config.secret_key.get_secret_value(),
            verify=self._config.verify,
            config=AioConfig(
                s3={"addressing_style": self._config.addressing_style, "payload_signing_enabled": False},
                request_checksum_calculation="when_required",
                max_pool_connections=self._config.max_pool_connections,
                connect_timeout=self._config.connect_timeout,
                read_timeout=self._config.read_timeout,
                retries={"max_attempts": self._config.max_attempts},
            ),
        ).__aenter__()
        logger.info("Backend client started", backend=self.name, endpoint=self._config.endpoint_url)

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

        method = getattr(self._client, operation.value)

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
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure()
            raise
        else:
            duration = time.perf_counter() - start_time
            self._metrics.record_backend_request(self.name, operation.value, "success", duration)
            self._metrics.record_backend_status(self.name, True)
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_success()
            return result
