"""Pure ASGI application that proxies S3 requests to configured backends."""

from __future__ import annotations

from typing import Any

from s3m.backends.pool import BackendPool
from s3m.common.errors import S3ErrorResponse, S3Errors
from s3m.common.logging import get_logger, setup_logging
from s3m.config.settings import load_settings
from s3m.handlers.buckets import (
    handle_create_bucket,
    handle_delete_bucket,
    handle_head_bucket,
    handle_list_buckets,
)
from s3m.handlers.objects import (
    handle_delete_object,
    handle_get_object,
    handle_head_object,
    handle_put_object,
)
from s3m.kafka.broker import create_broker
from s3m.kafka.publisher import ReplicationPublisher
from s3m.routing.classifier import classify_request
from s3m.routing.operations import S3Operation
from s3m.strategies.read import ReadFallbackStrategy
from s3m.strategies.write import WritePrimaryReplicationStrategy

logger = get_logger(__name__)


class S3ProxyApp:
    """
    Pure ASGI application that intercepts all HTTP requests,
    classifies them as S3 operations, and dispatches to the
    appropriate handler via read/write strategies.

    This bypasses Litestar's routing entirely — every request
    is an S3 API call routed through our classifier.
    """

    def __init__(self) -> None:
        self._pool: BackendPool | None = None
        self._read_strategy: ReadFallbackStrategy | None = None
        self._write_strategy: WritePrimaryReplicationStrategy | None = None
        self._broker: Any = None
        self._started = False

    async def startup(self) -> None:
        """Initialize all components. Called once by the ASGI server."""
        settings = load_settings()
        setup_logging(settings.log_level)

        log = get_logger("s3m.startup")
        log.info("Starting s3m proxy", backends=[b.name for b in settings.backends])

        # Backend pool
        self._pool = BackendPool(settings.backends)
        await self._pool.start()

        # Kafka
        self._broker = create_broker(settings.kafka)
        await self._broker.start()
        publisher = ReplicationPublisher(self._broker, settings.kafka.topic)

        # Strategies
        self._read_strategy = ReadFallbackStrategy()
        self._write_strategy = WritePrimaryReplicationStrategy(publisher)

        self._started = True
        log.info("s3m proxy ready")

    async def shutdown(self) -> None:
        """Clean up resources. Called once by the ASGI server."""
        log = get_logger("s3m.shutdown")
        log.info("Shutting down s3m proxy")

        if self._broker:
            await self._broker.close()
        if self._pool:
            await self._pool.close()

        log.info("s3m proxy stopped")

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI entry point."""
        if scope["type"] == "lifespan":
            await self._handle_lifespan(scope, receive, send)
            return

        if scope["type"] != "http":
            return

        await self._handle_http(scope, receive, send)

    async def _handle_lifespan(self, scope: dict, receive: Any, send: Any) -> None:
        """Handle ASGI lifespan events (startup/shutdown)."""
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

    async def _handle_http(self, scope: dict, receive: Any, send: Any) -> None:
        """Handle an HTTP request by classifying and dispatching it."""
        method = scope["method"]
        path = scope["path"]

        # Parse headers into a dict
        headers: dict[str, str] = {}
        for name_bytes, value_bytes in scope.get("headers", []):
            headers[name_bytes.decode("latin-1").lower()] = value_bytes.decode("latin-1")

        # Classify
        try:
            s3_req = classify_request(method, path)
        except ValueError:
            response = S3ErrorResponse(
                error_code=S3Errors.METHOD_NOT_ALLOWED,
                resource=path,
            ).to_response()
            await response(scope, receive, send)
            return

        # Dispatch
        try:
            response = await self._dispatch(
                s3_req.operation, s3_req.bucket, s3_req.key, receive, headers
            )
        except Exception as exc:
            logger.exception("Unhandled error in S3 proxy", error=str(exc))
            response = S3ErrorResponse(
                error_code=S3Errors.INTERNAL_ERROR,
                resource=path,
            ).to_response()

        await response(scope, receive, send)

    async def _dispatch(
        self,
        operation: S3Operation,
        bucket: str | None,
        key: str | None,
        receive: Any,
        headers: dict[str, str],
    ) -> Any:
        """Dispatch an S3 operation to the appropriate handler."""
        assert self._pool is not None
        assert self._read_strategy is not None
        assert self._write_strategy is not None

        pool = self._pool
        read_strategy = self._read_strategy
        write_strategy = self._write_strategy

        match operation:
            # Bucket operations
            case S3Operation.CREATE_BUCKET:
                return await handle_create_bucket(bucket, pool, write_strategy)
            case S3Operation.DELETE_BUCKET:
                return await handle_delete_bucket(bucket, pool, write_strategy)
            case S3Operation.HEAD_BUCKET:
                return await handle_head_bucket(bucket, pool, read_strategy)
            case S3Operation.LIST_BUCKETS:
                return await handle_list_buckets(pool, read_strategy)

            # Object operations
            case S3Operation.PUT_OBJECT:
                body = await _read_body(receive)
                content_type = headers.get("content-type", "application/octet-stream")
                return await handle_put_object(
                    bucket, key, body, pool, write_strategy, content_type
                )
            case S3Operation.GET_OBJECT:
                return await handle_get_object(bucket, key, pool, read_strategy)
            case S3Operation.DELETE_OBJECT:
                return await handle_delete_object(bucket, key, pool, write_strategy)
            case S3Operation.HEAD_OBJECT:
                return await handle_head_object(bucket, key, pool, read_strategy)

            case _:
                return S3ErrorResponse(
                    error_code=S3Errors.METHOD_NOT_ALLOWED,
                    message=f"Operation {operation} not implemented",
                ).to_response()


async def _read_body(receive: Any) -> bytes:
    """Read the full request body from ASGI receive."""
    chunks: list[bytes] = []
    while True:
        message = await receive()
        body = message.get("body", b"")
        if body:
            chunks.append(body)
        if not message.get("more_body", False):
            break
    return b"".join(chunks)


def create_app() -> S3ProxyApp:
    """Create the ASGI application."""
    return S3ProxyApp()
