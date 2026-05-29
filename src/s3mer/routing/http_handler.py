"""HTTP handler for the S3 proxy."""

import time
import uuid
from collections.abc import Callable, MutableMapping
from http import HTTPStatus
from typing import Any, ClassVar

import structlog

from s3mer.common.errors import S3ErrorResponse, S3Errors
from s3mer.common.logging import get_logger
from s3mer.common.responses import ASGIResponse
from s3mer.common.types import Receive, Scope, Send
from s3mer.handlers.internal import health_handler, metrics_handler
from s3mer.routing.classifier import RequestClassifier, S3Request
from s3mer.routing.dispatcher import RequestDispatcher

logger = get_logger(__name__)


class S3HTTPHandler:
    """Handles HTTP requests, request classification, and S3 request dispatching."""

    _internal_routes: ClassVar[dict[str, dict[str, Callable[[Scope, Receive, Send], Any]]]] = {
        "GET": {
            "/.internal/metrics": metrics_handler,
            "/.internal/health": health_handler,
        }
    }

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

        route_handler = self._internal_routes.get(method, {}).get(path)
        if route_handler is not None:
            await route_handler(scope, receive, send)
            return

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

        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}

        request_id = headers.get("x-s3mer-request-id")
        if not request_id:
            request_id = str(uuid.uuid4())

        structlog.contextvars.bind_contextvars(request_id=request_id)

        async def wrapped_send(event: MutableMapping[str, Any]) -> None:
            if event.get("type") == "http.response.start":
                event_headers = list(event.get("headers", []))
                if not any(k.lower() == b"x-s3mer-request-id" for k, _ in event_headers):
                    event_headers.append((b"x-s3mer-request-id", request_id.encode("latin-1")))
                event["headers"] = event_headers
            await send(event)

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
            s3_req, status_code = await self._classify_request(
                scope, receive, wrapped_send, method, path, query_string, headers, request_id
            )
            if s3_req is None:
                return

            operation_name = s3_req.operation.value

            response = await self._dispatch_request(s3_req, receive, headers, query_string, request_id, path)

            if hasattr(response, "extra_headers"):
                response.extra_headers["x-s3mer-request-id"] = request_id

            if (
                operation_name in ("put_object", "upload_part")
                and getattr(response, "status_code", 200) >= HTTPStatus.BAD_REQUEST
                and hasattr(response, "extra_headers")
            ):
                response.extra_headers["connection"] = "close"

            status_code = getattr(response, "status_code", 500)

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
