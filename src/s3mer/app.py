"""Pure ASGI application that proxies S3 requests to configured backends."""

import time
from http import HTTPStatus
from typing import Any

from s3mer.backends.pool import BackendPool
from s3mer.common.errors import S3ErrorResponse, S3Errors
from s3mer.common.logging import get_logger, setup_logging
from s3mer.common.metrics import get_tracker
from s3mer.common.types import Receive, Scope, Send
from s3mer.config.settings import load_settings
from s3mer.handlers.internal import health_handler, metrics_handler
from s3mer.kafka.broker import create_broker
from s3mer.kafka.manager import BatchReplicationManager, PerBackendReplicationManager
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.routing.classifier import RequestClassifier
from s3mer.routing.dispatcher import RequestDispatcher
from s3mer.strategies.read import ReadFallbackStrategy
from s3mer.strategies.write import WritePrimaryReplicationStrategy

logger = get_logger(__name__)


class S3ProxyApp:
    """
    Pure ASGI application that intercepts all HTTP requests,
    classifies them as S3 operations, and dispatches to the
    appropriate handler via read/write strategies.
    """

    def __init__(self) -> None:
        self._pool: BackendPool | None = None
        self._read_strategy: ReadFallbackStrategy | None = None
        self._write_strategy: WritePrimaryReplicationStrategy | None = None
        self._dispatcher: RequestDispatcher | None = None
        self._broker: Any = None
        self._metrics = get_tracker()
        self._classifier = RequestClassifier()
        self._started = False

    async def startup(self) -> None:
        """Initialize all components. Called once by the ASGI server."""
        settings = load_settings()
        setup_logging(settings.log_level)

        log = get_logger("s3mer.startup")
        log.info("Starting s3mer proxy", backends=[b.name for b in settings.backends])

        # Backend pool
        self._pool = BackendPool(settings.backends, self._metrics)
        await self._pool.start()

        # Kafka
        self._broker = create_broker(settings.kafka)
        await self._broker.start()
        publisher = ReplicationPublisher(self._broker, settings.kafka.topic)

        # Select replication manager based on configuration
        if settings.replication_mode == "per_backend":
            replication_manager = PerBackendReplicationManager(publisher, self._metrics)
        else:
            replication_manager = BatchReplicationManager(publisher, self._metrics)

        # Strategies
        self._read_strategy = ReadFallbackStrategy()
        self._write_strategy = WritePrimaryReplicationStrategy(replication_manager, self._metrics)

        # Dispatcher
        self._dispatcher = RequestDispatcher(self._pool, self._read_strategy, self._write_strategy, self._metrics)

        self._started = True
        log.info("s3mer proxy ready")

    async def shutdown(self) -> None:
        """Clean up resources. Called once by the ASGI server."""
        log = get_logger("s3mer.shutdown")
        log.info("Shutting down s3mer proxy")

        if self._broker:
            await self._broker.close()
        if self._pool:
            await self._pool.close()

        log.info("s3mer proxy stopped")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point."""
        if scope["type"] == "lifespan":
            await self._handle_lifespan(scope, receive, send)
            return

        if scope["type"] != "http":
            return

        await self._handle_http(scope, receive, send)

    async def _handle_lifespan(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle ASGI lifespan events (startup/shutdown)."""
        del scope
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    await self.startup()
                    await send({"type": "lifespan.startup.complete"})
                except Exception as exc:
                    logger.exception("Startup failed", error=str(exc))
                    await send({"type": "lifespan.startup.failed", "message": str(exc)})
                    return
            elif message["type"] == "lifespan.shutdown":
                await self.shutdown()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _handle_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle an HTTP request by classifying and dispatching it."""
        method = scope["method"]
        path = scope["path"]

        # Fast-path for internal service endpoints
        if path.startswith("/.internal/"):
            if method == "GET" and path == "/.internal/metrics":
                await metrics_handler(scope, receive, send)
                return
            if method == "GET" and path == "/.internal/health":
                await health_handler(scope, receive, send)
                return

            # Unknown internal endpoint
            response = S3ErrorResponse(
                error_code=S3Errors.ACCESS_DENIED,
                resource=path,
                message="Unknown internal endpoint",
            ).to_response()
            await response(scope, receive, send)
            return

        query_string = scope.get("query_string", b"")

        # Parse headers into a dict
        headers: dict[str, str] = {}
        for name_bytes, value_bytes in scope.get("headers", []):
            headers[name_bytes.decode("latin-1").lower()] = value_bytes.decode("latin-1")

        start_time = time.perf_counter()
        operation_name = "unknown"
        status_code = 500

        try:
            # 1. Classify
            try:
                s3_req = self._classifier.classify(method, path, query_string, headers)
            except ValueError:
                response = S3ErrorResponse(
                    error_code=S3Errors.METHOD_NOT_ALLOWED,
                    resource=path,
                ).to_response()
                status_code = getattr(response, "status_code", 405)
                await response(scope, receive, send)
                return

            operation_name = s3_req.operation.value
            scope["s3mer.operation"] = operation_name

            # 2. Dispatch
            if self._dispatcher is None:
                response = S3ErrorResponse(
                    error_code=S3Errors.INTERNAL_ERROR,
                    message="Proxy not initialized",
                ).to_response()
            else:
                try:
                    response = await self._dispatcher.dispatch(s3_req, receive, headers, query_string)
                except Exception as exc:
                    logger.exception("Unhandled error in S3 proxy", error=str(exc))
                    response = S3ErrorResponse.from_client_error(exc, resource=path).to_response()

            # 3. Connection management for failures during body reads
            if (
                operation_name in ("put_object", "upload_part")
                and getattr(response, "status_code", 200) >= HTTPStatus.BAD_REQUEST
                and hasattr(response, "extra_headers")
            ):
                response.extra_headers["connection"] = "close"

            status_code = getattr(response, "status_code", 500)

            # 4. Metrics callback for outbound data transfer
            def record_out_bytes(n: int) -> None:
                self._metrics.record_data_transfer(direction="out", operation=operation_name, bytes_count=n)

            if hasattr(response, "on_bytes_sent"):
                response.on_bytes_sent = record_out_bytes

            await response(scope, receive, send)
        finally:
            duration = time.perf_counter() - start_time
            self._metrics.record_request(method=method, operation=operation_name, status=status_code, duration=duration)


def create_app() -> S3ProxyApp:
    """Create the ASGI application."""
    return S3ProxyApp()
