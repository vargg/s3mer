"""Dispatch classified S3 operations to their respective handlers."""

from typing import Any

from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import ReadFallbackStrategy, WritePrimaryReplicationStrategy
from s3mer.common.errors import S3ErrorResponse, S3Errors
from s3mer.common.metrics import MetricsTracker
from s3mer.common.streaming import ASGIStreamReader, AWSChunkedDecoder
from s3mer.common.types import Receive
from s3mer.routing.classifier import S3Request
from s3mer.routing.registry import BodyStyle, HandlerContext, registry


class RequestDispatcher:
    """
    Dispatches S3 operations to handler functions using a central registry.

    This class provides a clean O(1) lookup-based dispatch mechanism
    and handles request body preparation based on the operation's BodyStyle.
    """

    def __init__(
        self,
        pool: BackendPool,
        read_strategy: ReadFallbackStrategy,
        write_strategy: WritePrimaryReplicationStrategy,
        metrics: MetricsTracker,
    ) -> None:
        self._pool = pool
        self._read_strategy = read_strategy
        self._write_strategy = write_strategy
        self._metrics = metrics

    async def dispatch(
        self,
        request: S3Request,
        receive: Receive,
        headers: dict[str, str],
        query_string: bytes,
    ) -> Any:
        """Main entry point for dispatching a classified request."""
        metadata = registry.get(request.operation)
        if not metadata:
            return S3ErrorResponse(
                error_code=S3Errors.METHOD_NOT_ALLOWED,
                message=f"Operation {request.operation} not implemented",
            ).to_response()

        # Object-level check: ensure key is present for object operations
        if metadata.is_object_op and request.key is None:
            return S3ErrorResponse(
                error_code=S3Errors.NO_SUCH_KEY,
                message="Object key is required",
                resource=f"/{request.bucket}",
            ).to_response()

        # 1. Prepare body based on BodyStyle
        body: Any = None
        content_length: int | None = None

        if metadata.body_style == BodyStyle.STREAM:
            body, content_length = self._prepare_streaming_body(receive, headers, request.operation.value)
        elif metadata.body_style == BodyStyle.BUFFERED:
            body = await self._read_body(receive, request.operation.value)

        # 2. Construct context
        ctx = HandlerContext(
            operation=request.operation,
            bucket=request.bucket or "",
            key=request.key,
            pool=self._pool,
            read_strategy=self._read_strategy,
            write_strategy=self._write_strategy,
            metrics=self._metrics,
            headers=headers,
            query_string=query_string,
            body=body,
            content_length=content_length,
        )

        # 3. Call handler
        return await metadata.func(ctx)

    # --- Helpers ---

    def _prepare_streaming_body(
        self, receive: Receive, headers: dict[str, str], operation_name: str
    ) -> tuple[Any, int | None]:
        """Sets up ASGIStreamReader and optional AWSChunkedDecoder."""
        content_length_str = headers.get("content-length")
        content_length = int(content_length_str) if content_length_str else None

        def on_read(n: int) -> None:
            self._metrics.record_data_transfer(direction="in", operation=operation_name, bytes_count=n)

        body: Any = ASGIStreamReader(receive, on_read=on_read)

        if headers.get("x-amz-content-sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
            body = AWSChunkedDecoder(body)
            decoded_length_str = headers.get("x-amz-decoded-content-length")
            if decoded_length_str:
                content_length = int(decoded_length_str)

        return body, content_length

    async def _read_body(self, receive: Receive, operation_name: str) -> bytes:
        """Read the entire request body into memory. Used for metadata/XML payloads."""
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                body.extend(chunk)
                if chunk:
                    self._metrics.record_data_transfer(direction="in", operation=operation_name, bytes_count=len(chunk))
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                break
        return bytes(body)
