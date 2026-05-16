"""Classify incoming HTTP requests into S3 operations."""

import re
from dataclasses import dataclass
from typing import ClassVar
from urllib.parse import parse_qsl

from s3mer.routing.operations import S3Operation


@dataclass(frozen=True, slots=True)
class S3Request:
    """Parsed S3 request with operation, bucket, and optional key."""

    operation: S3Operation
    bucket: str | None = None
    key: str | None = None


class RequestClassifier:
    """
    Classifies HTTP requests into S3 operations using fast path-style routing.

    This replaces the linear regex scan with an O(1) method-based lookup
    and delegated refinement logic for better performance and maintainability.
    """

    # S3 Bucket Naming Regex (3-63 chars, alphanumeric start/end, lowercase/numbers/dots/hyphens)
    _BUCKET_NAME_PATTERN = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
    _BUCKET_NAME_RE = re.compile(_BUCKET_NAME_PATTERN)

    # Base routing table: (Method, Depth) -> Base Operation
    # Depth 0: Service level (/)
    # Depth 1: Bucket level (/{bucket})
    # Depth 2: Object level (/{bucket}/{key})
    _ROUTING_TABLE: ClassVar[dict[str, dict[int, S3Operation]]] = {
        "GET": {
            0: S3Operation.LIST_BUCKETS,
            1: S3Operation.LIST_OBJECTS_V2,
            2: S3Operation.GET_OBJECT,
        },
        "PUT": {
            1: S3Operation.CREATE_BUCKET,
            2: S3Operation.PUT_OBJECT,
        },
        "DELETE": {
            1: S3Operation.DELETE_BUCKET,
            2: S3Operation.DELETE_OBJECT,
        },
        "HEAD": {
            1: S3Operation.HEAD_BUCKET,
            2: S3Operation.HEAD_OBJECT,
        },
        "POST": {
            1: S3Operation.DELETE_OBJECTS,
            2: S3Operation.POST_OBJECT,
        },
    }

    def classify(
        self, method: str, path: str, query_string: bytes = b"", headers: dict[str, str] | None = None
    ) -> S3Request:
        """
        Classify an HTTP request into an S3 operation.
        """
        method = method.upper()
        bucket, key = self._extract_parts(path)

        # 1. Determine base operation from (method, depth)
        depth = 0 if not bucket else (2 if key else 1)
        base_op = self._ROUTING_TABLE.get(method, {}).get(depth)

        if base_op is None:
            raise ValueError(f"Cannot classify request: {method} {path}")

        # 2. Validate bucket if present
        if bucket and not self._BUCKET_NAME_RE.match(bucket):
            raise ValueError(f"Invalid bucket name: {bucket}")

        # 3. Refine operation based on query parameters and headers
        query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))
        operation = self._refine_operation(base_op, query, headers, path)

        return S3Request(operation=operation, bucket=bucket, key=key)

    def _extract_parts(self, path: str) -> tuple[str | None, str | None]:
        """Fast path-style extraction of bucket and key."""
        parts = path.strip("/").split("/", 1)
        if not parts or not parts[0]:
            return None, None

        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else None
        return bucket, key

    def _refine_operation(
        self, base_op: S3Operation, query: dict[str, str], headers: dict[str, str] | None, path: str
    ) -> S3Operation:
        """Dispatch to specialized refinement logic based on the base operation's method."""
        match base_op:
            case S3Operation.PUT_OBJECT:
                return self._refine_put(query, headers)
            case S3Operation.POST_OBJECT:
                return self._refine_post(query, path)
            case S3Operation.GET_OBJECT:
                return self._refine_get(query)
            case S3Operation.DELETE_OBJECT:
                return self._refine_delete(query)
            case S3Operation.DELETE_OBJECTS:
                if "delete" not in query:
                    raise ValueError(f"Cannot classify POST request without 'delete' query param: {path}")
                return base_op
            case S3Operation.LIST_OBJECTS_V2:
                if "list-type" not in query or query["list-type"] != "2":
                    return S3Operation.LIST_OBJECTS
                return base_op
            case _:
                return base_op

    def _refine_put(self, query: dict[str, str], headers: dict[str, str] | None) -> S3Operation:
        if "tagging" in query:
            return S3Operation.PUT_OBJECT_TAGGING
        if "partNumber" in query and "uploadId" in query:
            return S3Operation.UPLOAD_PART
        if headers and "x-amz-copy-source" in headers:
            return S3Operation.COPY_OBJECT
        return S3Operation.PUT_OBJECT

    def _refine_post(self, query: dict[str, str], path: str) -> S3Operation:
        if "uploads" in query:
            return S3Operation.CREATE_MULTIPART_UPLOAD
        if "uploadId" in query:
            return S3Operation.COMPLETE_MULTIPART_UPLOAD
        raise ValueError(f"Cannot classify POST request without uploads or uploadId: {path}")

    def _refine_get(self, query: dict[str, str]) -> S3Operation:
        if "tagging" in query:
            return S3Operation.GET_OBJECT_TAGGING
        return S3Operation.GET_OBJECT

    def _refine_delete(self, query: dict[str, str]) -> S3Operation:
        if "tagging" in query:
            return S3Operation.DELETE_OBJECT_TAGGING
        if "uploadId" in query:
            return S3Operation.ABORT_MULTIPART_UPLOAD
        return S3Operation.DELETE_OBJECT
