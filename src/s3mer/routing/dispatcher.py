"""Dispatch classified S3 operations to their respective handlers."""

from typing import Any

from s3mer.backends.pool import BackendPool
from s3mer.common.errors import S3ErrorResponse, S3Errors
from s3mer.common.metrics import MetricsTracker
from s3mer.common.streaming import ASGIStreamReader, AWSChunkedDecoder
from s3mer.common.types import Receive
from s3mer.handlers.buckets import (
    handle_create_bucket,
    handle_delete_bucket,
    handle_delete_objects,
    handle_head_bucket,
    handle_list_buckets,
    handle_list_objects,
    handle_list_objects_v2,
)
from s3mer.handlers.objects import (
    handle_abort_multipart_upload,
    handle_complete_multipart_upload,
    handle_copy_object,
    handle_create_multipart_upload,
    handle_delete_object,
    handle_delete_object_tagging,
    handle_get_object,
    handle_get_object_tagging,
    handle_head_object,
    handle_put_object,
    handle_put_object_tagging,
    handle_upload_part,
)
from s3mer.routing.classifier import S3Request
from s3mer.routing.operations import S3Operation
from s3mer.strategies.read import ReadFallbackStrategy
from s3mer.strategies.write import WritePrimaryReplicationStrategy


class RequestDispatcher:
    """
    Dispatches S3 operations to handler functions.

    This class decouples the S3ProxyApp from the specific arguments
    required by each handler, providing a clean O(1) lookup-based
    dispatch mechanism.
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

        # Dispatch table for base operations
        self._handlers = {
            S3Operation.LIST_BUCKETS: self._dispatch_list_buckets,
            S3Operation.CREATE_BUCKET: self._dispatch_create_bucket,
            S3Operation.DELETE_BUCKET: self._dispatch_delete_bucket,
            S3Operation.HEAD_BUCKET: self._dispatch_head_bucket,
            S3Operation.LIST_OBJECTS: self._dispatch_list_objects,
            S3Operation.LIST_OBJECTS_V2: self._dispatch_list_objects_v2,
            S3Operation.DELETE_OBJECTS: self._dispatch_delete_objects,
            # Object operations
            S3Operation.GET_OBJECT: self._dispatch_get_object,
            S3Operation.PUT_OBJECT: self._dispatch_put_object,
            S3Operation.DELETE_OBJECT: self._dispatch_delete_object,
            S3Operation.HEAD_OBJECT: self._dispatch_head_object,
            S3Operation.COPY_OBJECT: self._dispatch_copy_object,
            S3Operation.PUT_OBJECT_TAGGING: self._dispatch_put_object_tagging,
            S3Operation.GET_OBJECT_TAGGING: self._dispatch_get_object_tagging,
            S3Operation.DELETE_OBJECT_TAGGING: self._dispatch_delete_object_tagging,
            S3Operation.CREATE_MULTIPART_UPLOAD: self._dispatch_create_multipart_upload,
            S3Operation.UPLOAD_PART: self._dispatch_upload_part,
            S3Operation.COMPLETE_MULTIPART_UPLOAD: self._dispatch_complete_multipart_upload,
            S3Operation.ABORT_MULTIPART_UPLOAD: self._dispatch_abort_multipart_upload,
        }

    async def dispatch(
        self,
        request: S3Request,
        receive: Receive,
        headers: dict[str, str],
        query_string: bytes,
    ) -> Any:
        """Main entry point for dispatching a classified request."""
        handler = self._handlers.get(request.operation)
        if not handler:
            return S3ErrorResponse(
                error_code=S3Errors.METHOD_NOT_ALLOWED,
                message=f"Operation {request.operation} not implemented",
            ).to_response()

        # Object-level check: ensure key is present for object operations
        if (
            request.operation
            not in (
                S3Operation.LIST_BUCKETS,
                S3Operation.CREATE_BUCKET,
                S3Operation.DELETE_BUCKET,
                S3Operation.HEAD_BUCKET,
                S3Operation.LIST_OBJECTS,
                S3Operation.LIST_OBJECTS_V2,
                S3Operation.DELETE_OBJECTS,
            )
            and request.key is None
        ):
            return S3ErrorResponse(
                error_code=S3Errors.NO_SUCH_KEY,
                message="Object key is required",
                resource=f"/{request.bucket}",
            ).to_response()

        return await handler(request, receive, headers, query_string)

    # --- Bucket Handler Wrappers ---

    async def _dispatch_list_buckets(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del req, receive, headers, query
        return await handle_list_buckets(self._pool, self._read_strategy)

    async def _dispatch_create_bucket(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_create_bucket(req.bucket or "", self._pool, self._write_strategy)

    async def _dispatch_delete_bucket(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_delete_bucket(req.bucket or "", self._pool, self._write_strategy)

    async def _dispatch_head_bucket(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_head_bucket(req.bucket or "", self._pool, self._read_strategy)

    async def _dispatch_list_objects(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers
        return await handle_list_objects(req.bucket or "", self._pool, self._read_strategy, query)

    async def _dispatch_list_objects_v2(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers
        return await handle_list_objects_v2(req.bucket or "", self._pool, self._read_strategy, query)

    async def _dispatch_delete_objects(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del headers, query
        body = await self._read_body(receive, req.operation.value)
        return await handle_delete_objects(req.bucket or "", self._pool, self._write_strategy, body)

    # --- Object Handler Wrappers ---

    async def _dispatch_get_object(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_get_object(req.bucket or "", req.key or "", self._pool, self._read_strategy)

    async def _dispatch_put_object(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del query
        body, content_length = self._prepare_streaming_body(receive, headers, req.operation.value)
        content_type = headers.get("content-type", "application/octet-stream")
        return await handle_put_object(
            req.bucket or "", req.key or "", body, self._pool, self._write_strategy, content_type, content_length
        )

    async def _dispatch_delete_object(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_delete_object(req.bucket or "", req.key or "", self._pool, self._write_strategy)

    async def _dispatch_head_object(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_head_object(req.bucket or "", req.key or "", self._pool, self._read_strategy)

    async def _dispatch_copy_object(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, query
        copy_source = headers.get("x-amz-copy-source", "")
        return await handle_copy_object(req.bucket or "", req.key or "", self._pool, self._write_strategy, copy_source)

    async def _dispatch_put_object_tagging(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del headers, query
        body = await self._read_body(receive, req.operation.value)
        return await handle_put_object_tagging(req.bucket or "", req.key or "", self._pool, self._write_strategy, body)

    async def _dispatch_get_object_tagging(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_get_object_tagging(req.bucket or "", req.key or "", self._pool, self._read_strategy)

    async def _dispatch_delete_object_tagging(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers, query
        return await handle_delete_object_tagging(req.bucket or "", req.key or "", self._pool, self._write_strategy)

    async def _dispatch_create_multipart_upload(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, query
        return await handle_create_multipart_upload(
            req.bucket or "", req.key or "", self._pool, self._write_strategy, headers
        )

    async def _dispatch_upload_part(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        body, content_length = self._prepare_streaming_body(receive, headers, req.operation.value)
        return await handle_upload_part(
            req.bucket or "", req.key or "", body, self._pool, self._write_strategy, query, content_length
        )

    async def _dispatch_complete_multipart_upload(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del headers
        body = await self._read_body(receive, req.operation.value)
        return await handle_complete_multipart_upload(
            req.bucket or "", req.key or "", body, self._pool, self._write_strategy, query
        )

    async def _dispatch_abort_multipart_upload(
        self, req: S3Request, receive: Receive, headers: dict[str, str], query: bytes
    ) -> Any:
        del receive, headers
        return await handle_abort_multipart_upload(
            req.bucket or "", req.key or "", self._pool, self._write_strategy, query
        )

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
