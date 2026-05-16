"""Classify incoming HTTP requests into S3 operations."""

import re
from dataclasses import dataclass
from urllib.parse import parse_qsl

from s3m.routing.operations import S3Operation


@dataclass(frozen=True, slots=True)
class S3Request:
    """Parsed S3 request with operation, bucket, and optional key."""

    operation: S3Operation
    bucket: str | None = None
    key: str | None = None


# Path-style URL patterns (order matters — more specific first)
_ROUTE_PATTERNS: list[tuple[str, re.Pattern[str], S3Operation]] = [
    # Object operations: /{bucket}/{key...}
    ("PUT", re.compile(r"^/([^/]+)/(.+)$"), S3Operation.PUT_OBJECT),
    ("GET", re.compile(r"^/([^/]+)/(.+)$"), S3Operation.GET_OBJECT),
    ("DELETE", re.compile(r"^/([^/]+)/(.+)$"), S3Operation.DELETE_OBJECT),
    ("HEAD", re.compile(r"^/([^/]+)/(.+)$"), S3Operation.HEAD_OBJECT),
    ("POST", re.compile(r"^/([^/]+)/(.+)$"), S3Operation.POST_OBJECT),
    # Bucket operations: /{bucket}
    ("POST", re.compile(r"^/([^/]+)/?$"), S3Operation.DELETE_OBJECTS),
    ("PUT", re.compile(r"^/([^/]+)/?$"), S3Operation.CREATE_BUCKET),
    ("DELETE", re.compile(r"^/([^/]+)/?$"), S3Operation.DELETE_BUCKET),
    ("HEAD", re.compile(r"^/([^/]+)/?$"), S3Operation.HEAD_BUCKET),
    ("GET", re.compile(r"^/([^/]+)/?$"), S3Operation.LIST_OBJECTS_V2),
    # Service operations: /
    ("GET", re.compile(r"^/?$"), S3Operation.LIST_BUCKETS),
]


def classify_request(  # noqa: PLR0912
    method: str, path: str, query_string: bytes = b"", headers: dict[str, str] | None = None
) -> S3Request:
    """
    Classify an HTTP request into an S3 operation.

    Uses path-style URL parsing: /{bucket}/{key}
    And parses query string to determine operation subtypes.

    Args:
        method: HTTP method (GET, PUT, DELETE, HEAD).
        path: URL path (e.g., "/my-bucket/photos/cat.jpg").
        query_string: Raw ASGI query string bytes.
        headers: Optional dictionary of HTTP headers.

    Returns:
        Parsed S3Request with operation, bucket, and key.

    Raises:
        ValueError: If the request cannot be mapped to a known S3 operation.
    """
    method = method.upper()
    query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))

    for route_method, pattern, base_operation in _ROUTE_PATTERNS:
        if method != route_method:
            continue

        match = pattern.match(path)
        if not match:
            continue

        groups = match.groups()
        bucket = groups[0] if len(groups) > 0 else None
        key = groups[1] if len(groups) > 1 else None

        # Refine operation based on query parameters
        operation = base_operation

        if base_operation == S3Operation.POST_OBJECT:
            if "uploads" in query:
                operation = S3Operation.CREATE_MULTIPART_UPLOAD
            elif "uploadId" in query:
                operation = S3Operation.COMPLETE_MULTIPART_UPLOAD
            else:
                raise ValueError(f"Cannot classify POST request without uploads or uploadId: {path}")

        elif base_operation == S3Operation.PUT_OBJECT:
            if "tagging" in query:
                operation = S3Operation.PUT_OBJECT_TAGGING
            elif "partNumber" in query and "uploadId" in query:
                operation = S3Operation.UPLOAD_PART
            elif headers and "x-amz-copy-source" in headers:
                operation = S3Operation.COPY_OBJECT

        elif base_operation == S3Operation.GET_OBJECT:
            if "tagging" in query:
                operation = S3Operation.GET_OBJECT_TAGGING

        elif base_operation == S3Operation.DELETE_OBJECT:
            if "tagging" in query:
                operation = S3Operation.DELETE_OBJECT_TAGGING
            elif "uploadId" in query:
                operation = S3Operation.ABORT_MULTIPART_UPLOAD

        elif base_operation == S3Operation.DELETE_OBJECTS and "delete" not in query:
            msg = f"Cannot classify POST request without 'delete' query param: {path}"
            raise ValueError(msg)

        elif base_operation == S3Operation.LIST_OBJECTS_V2 and ("list-type" not in query or query["list-type"] != "2"):
            operation = S3Operation.LIST_OBJECTS

        return S3Request(operation=operation, bucket=bucket, key=key)

    msg = f"Cannot classify request: {method} {path}"
    raise ValueError(msg)
