"""In-memory S3 backend for local development and tests without MinIO."""

import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)

_MEMORY_STORE: dict[str, dict[str, dict[str, Any]]] = {}


class MemoryS3BackendClient:
    """Minimal in-memory S3 client implementing a subset of operations."""

    def __init__(self, name: str, is_primary: bool, priority: int, metrics: MetricsTracker) -> None:
        self.name = name
        self.is_primary = is_primary
        self.priority = priority
        self.last_latency: float = 0.0
        self._metrics = metrics
        self._circuit_breaker = None

    def set_circuit_breaker(self, breaker: Any) -> None:
        self._circuit_breaker = breaker

    async def start(self) -> None:
        logger.info("Memory backend started", backend=self.name)

    async def close(self) -> None:
        logger.info("Memory backend closed", backend=self.name)

    def _bucket_objects(self, bucket: str) -> dict[str, dict[str, Any]]:
        return _MEMORY_STORE.setdefault(self.name, {}).setdefault(bucket, {})

    async def execute(self, operation: S3Operation, params: dict[str, Any]) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            result = self._dispatch(operation, params)
        except Exception:
            self._metrics.record_backend_request(self.name, operation.value, "error", time.perf_counter() - start)
            self._metrics.record_backend_status(self.name, False)
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure()
            raise
        else:
            self._metrics.record_backend_request(self.name, operation.value, "success", time.perf_counter() - start)
            self._metrics.record_backend_status(self.name, True)
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_success()
            return result

    def _dispatch(self, operation: S3Operation, params: dict[str, Any]) -> dict[str, Any]:  # noqa: PLR0912
        bucket = params["Bucket"]
        match operation:
            case S3Operation.CREATE_BUCKET:
                self._bucket_objects(bucket)
                return {}
            case S3Operation.DELETE_BUCKET:
                store = _MEMORY_STORE.get(self.name, {})
                if bucket not in store:
                    raise _client_error("NoSuchBucket", 404)
                if store[bucket]:
                    raise _client_error("BucketNotEmpty", 409)
                del store[bucket]
                return {}
            case S3Operation.PUT_OBJECT:
                key = params["Key"]
                body = params.get("Body", b"")
                if hasattr(body, "read"):
                    data = body.read() if not hasattr(body, "__aiter__") else b"".join([])
                elif hasattr(body, "__aiter__"):
                    data = b"".join([])
                else:
                    data = bytes(body) if body is not None else b""
                etag = f'"{uuid4().hex}"'
                obj = {
                    "Body": data,
                    "ContentLength": len(data),
                    "ETag": etag,
                    "LastModified": datetime.now(tz=UTC),
                    "ContentType": params.get("ContentType", "application/octet-stream"),
                }
                for meta_key in (
                    "ContentEncoding",
                    "CacheControl",
                    "ContentDisposition",
                    "ContentLanguage",
                    "Metadata",
                ):
                    if meta_key in params:
                        obj[meta_key] = params[meta_key]
                self._bucket_objects(bucket)[key] = obj
                return {"ETag": etag}
            case S3Operation.GET_OBJECT:
                key = params["Key"]
                obj = self._bucket_objects(bucket).get(key)
                if obj is None:
                    raise _client_error("NoSuchKey", 404)
                return dict(obj)
            case S3Operation.HEAD_OBJECT:
                key = params["Key"]
                obj = self._bucket_objects(bucket).get(key)
                if obj is None:
                    raise _client_error("NoSuchKey", 404)
                return {k: v for k, v in obj.items() if k != "Body"}
            case S3Operation.DELETE_OBJECT:
                key = params["Key"]
                objects = self._bucket_objects(bucket)
                if key not in objects:
                    raise _client_error("NoSuchKey", 404)
                del objects[key]
                return {}
            case S3Operation.LIST_BUCKETS:
                buckets = [{"Name": name} for name in _MEMORY_STORE.get(self.name, {})]
                return {"Buckets": buckets}
            case _:
                raise _client_error("NotImplemented", 501)


def _client_error(code: str, status: int) -> Exception:
    from botocore.exceptions import ClientError  # noqa: PLC0415

    return ClientError(
        error_response={
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation_name="MemoryBackend",
    )


def clear_memory_store() -> None:
    """Reset all in-memory backend data (for tests)."""
    _MEMORY_STORE.clear()
