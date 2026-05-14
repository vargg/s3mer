"""Classify incoming HTTP requests into S3 operations."""

from __future__ import annotations

import re
from dataclasses import dataclass

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
    # Bucket operations: /{bucket}
    ("PUT", re.compile(r"^/([^/]+)/?$"), S3Operation.CREATE_BUCKET),
    ("DELETE", re.compile(r"^/([^/]+)/?$"), S3Operation.DELETE_BUCKET),
    ("HEAD", re.compile(r"^/([^/]+)/?$"), S3Operation.HEAD_BUCKET),
    # Service operations: /
    ("GET", re.compile(r"^/?$"), S3Operation.LIST_BUCKETS),
]


def classify_request(method: str, path: str) -> S3Request:
    """
    Classify an HTTP request into an S3 operation.

    Uses path-style URL parsing: /{bucket}/{key}

    Args:
        method: HTTP method (GET, PUT, DELETE, HEAD).
        path: URL path (e.g., "/my-bucket/photos/cat.jpg").

    Returns:
        Parsed S3Request with operation, bucket, and key.

    Raises:
        ValueError: If the request cannot be mapped to a known S3 operation.
    """
    method = method.upper()

    for route_method, pattern, operation in _ROUTE_PATTERNS:
        if method != route_method:
            continue

        match = pattern.match(path)
        if not match:
            continue

        groups = match.groups()
        bucket = groups[0] if len(groups) > 0 else None
        key = groups[1] if len(groups) > 1 else None

        return S3Request(operation=operation, bucket=bucket, key=key)

    msg = f"Cannot classify request: {method} {path}"
    raise ValueError(msg)
