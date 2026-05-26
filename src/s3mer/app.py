"""Pure ASGI application that proxies S3 requests to configured backends."""

import time
import uuid
from collections.abc import MutableMapping
from http import HTTPStatus
from typing import Any

import structlog

from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import (
    MultiSyncWriteStrategy,
    ReadFallbackStrategy,
    WritePrimaryReplicationStrategy,
)
from s3mer.common.errors import S3ErrorResponse, S3Errors
from s3mer.common.logging import get_logger, setup_logging
from s3mer.common.metrics import get_tracker
from s3mer.common.responses import ASGIResponse
from s3mer.common.types import Receive, Scope, Send
from s3mer.config.settings import ReplicationMode, WriteStrategyType, load_settings
from s3mer.handlers.internal import health_handler, metrics_handler
from s3mer.kafka.broker import create_broker
from s3mer.kafka.manager import BatchReplicationManager, PerBackendReplicationManager
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.routing.classifier import RequestClassifier, S3Request
from s3mer.routing.dispatcher import RequestDispatcher

logger = get_logger(__name__)


class S3HTTPHandler:
    """Handles HTTP requests, request classification, and S3 request dispatching."""

    def __init__(
        self,
        request_classifier: RequestClassifier,
        dispatcher: RequestDispatcher | None,
        metrics_tracker: Any,
    ) -> None:
        self._request_classifier = request_classifier
        self._dispatcher = dispatcher
        self._metrics_tracker = metrics_tracker

    async def _handle_internal_routes(self, scope: Scope, receive: Receive, send: Send, request_id: str) -> None:
        """Handle internal service endpoints (metrics, health, etc.)."""
        method = scope["method"]
        path = scope["path"]

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
            request_id=request_id,
        ).to_response()
        response.extra_headers["x-s3mer-request-id"] = request_id
        await response(scope, receive, send)

    async def _classify_request(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        method: str,
        path: str,
        query_string: bytes,
        headers: dict[str, str],
        request_id: str,
    ) -> tuple[S3Request | None, int | None]:
        """Classify the incoming request, returning the S3Request object and status code."""
        try:
            s3_req = self._request_classifier.classify(method, path, query_string, headers)
        except ValueError:
            response = S3ErrorResponse(
                error_code=S3Errors.METHOD_NOT_ALLOWED,
                resource=path,
                request_id=request_id,
            ).to_response()
            response.extra_headers["x-s3mer-request-id"] = request_id
            status_code = getattr(response, "status_code", 405)
            await response(scope, receive, send)
            return None, status_code
        else:
            return s3_req, None

    async def _dispatch_request(
        self,
        s3_req: S3Request,
        receive: Receive,
        headers: dict[str, str],
        query_string: bytes,
        request_id: str,
        path: str,
    ) -> ASGIResponse:
        """Dispatch the S3 request using the dispatcher or handle errors."""
        if self._dispatcher is None:
            return S3ErrorResponse(
                error_code=S3Errors.INTERNAL_ERROR,
                message="Proxy not initialized",
                request_id=request_id,
            ).to_response()
        try:
            return await self._dispatcher.dispatch(s3_req, receive, headers, query_string)
        except Exception as exc:
            logger.exception("Unhandled error in S3 proxy", error=str(exc))
            err_resp = S3ErrorResponse.from_client_error(exc, resource=path)
            err_resp.request_id = request_id
            return err_resp.to_response()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle an HTTP request by classifying and dispatching it."""
        path = scope["path"]

        # Parse headers into a dict
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}

        # Resolve or generate Request ID
        request_id = headers.get("x-s3mer-request-id")
        if not request_id:
            request_id = str(uuid.uuid4())
        scope["s3mer.request_id"] = request_id

        # Bind to structlog contextvars
        structlog.contextvars.bind_contextvars(request_id=request_id)

        # Define wrapped_send to guarantee request_id propagation in headers
        async def wrapped_send(event: MutableMapping[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                event_headers = list(event.get("headers", []))
                if not any(k.lower() == b"x-s3mer-request-id" for k, _ in event_headers):
                    event_headers.append((b"x-s3mer-request-id", request_id.encode("latin-1")))
                event["headers"] = event_headers
            await send(event)

        # Fast-path for internal service endpoints
        if path.startswith("/.internal/"):
            try:
                await self._handle_internal_routes(scope, receive, wrapped_send, request_id)
            finally:
                structlog.contextvars.clear_contextvars()
            return

        method = scope["method"]
        query_string = scope.get("query_string", b"")

        start_time = time.perf_counter()
        operation_name = "unknown"
        status_code = None

        try:
            # 1. Classify
            s3_req, status_code = await self._classify_request(
                scope, receive, wrapped_send, method, path, query_string, headers, request_id
            )
            if s3_req is None:
                return

            operation_name = s3_req.operation.value
            scope["s3mer.operation"] = operation_name

            # 2. Dispatch
            response = await self._dispatch_request(s3_req, receive, headers, query_string, request_id, path)

            # Ensure response has the request ID header
            if hasattr(response, "extra_headers"):
                response.extra_headers["x-s3mer-request-id"] = request_id

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
                self._metrics_tracker.record_data_transfer(direction="out", operation=operation_name, bytes_count=n)

            if hasattr(response, "on_bytes_sent"):
                response.on_bytes_sent = record_out_bytes

            await response(scope, receive, wrapped_send)
        finally:
            duration = time.perf_counter() - start_time
            self._metrics_tracker.record_request(
                method=method, operation=operation_name, status=status_code or 500, duration=duration
            )
            structlog.contextvars.clear_contextvars()


class S3ProxyApp:
    """
    Pure ASGI application that intercepts all HTTP requests,
    classifies them as S3 operations, and dispatches to the
    appropriate handler via read/write strategies.
    """

    def __init__(self) -> None:
        settings = load_settings()
        metrics_tracker = get_tracker()

        self._broker = create_broker(settings.kafka)
        self._pool = BackendPool(
            settings.backends,
            metrics_tracker,
            settings.latency_probe_interval_seconds,
        )

        publisher = ReplicationPublisher(self._broker, settings.kafka.topic)
        if settings.replication_mode == ReplicationMode.PER_BACKEND:
            replication_manager = PerBackendReplicationManager(publisher, metrics_tracker)
        else:
            replication_manager = BatchReplicationManager(publisher, metrics_tracker)

        if settings.write_strategy == WriteStrategyType.MULTI_SYNC:
            write_strategy: Any = MultiSyncWriteStrategy(metrics_tracker)
        else:
            write_strategy = WritePrimaryReplicationStrategy(replication_manager, metrics_tracker)

        dispatcher = RequestDispatcher(
            self._pool,
            ReadFallbackStrategy(),
            write_strategy,
            metrics_tracker,
        )

        self._http_handler = S3HTTPHandler(
            RequestClassifier(),
            dispatcher,
            metrics_tracker,
        )

    async def startup(self) -> None:
        """Initialize all components. Called once by the ASGI server."""
        settings = load_settings()
        setup_logging(settings.log_level, settings.log_file)

        log = get_logger("s3mer.startup")
        log.info("Starting s3mer proxy", backends=list(settings.backends.keys()))

        await self._pool.start()
        await self._broker.start()

        log.info("s3mer proxy ready")

    async def shutdown(self) -> None:
        """Clean up resources. Called once by the ASGI server."""
        log = get_logger("s3mer.shutdown")
        log.info("Shutting down s3mer proxy")

        await self._broker.stop()
        await self._pool.close()

        log.info("s3mer proxy stopped")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point."""
        if scope["type"] == "lifespan":
            await self._handle_lifespan(scope, receive, send)
            return

        if scope["type"] != "http":
            return

        await self._http_handler(scope, receive, send)

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


def create_app() -> S3ProxyApp:
    """Create the ASGI application."""
    return S3ProxyApp()
