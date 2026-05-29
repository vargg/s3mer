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


@dataclass(frozen=True, slots=True)
class _RefinementRule:
    """Declarative rule for refining a base S3 operation based on query params or headers."""

    refined_op: S3Operation
    query_key: str | None = None
    header_key: str | None = None
    extra_query_key: str | None = None

    def matches(self, query: dict[str, str], headers: dict[str, str] | None) -> bool:
        """Return True if this rule's conditions are satisfied by the request."""
        if self.query_key is not None and self.query_key not in query:
            return False
        if self.extra_query_key is not None and self.extra_query_key not in query:
            return False
        return not (self.header_key is not None and (headers is None or self.header_key not in headers))


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

    # Refinement table: base_op → ordered rules (first match wins).
    # Adding a new S3 sub-operation = one line here.
    _REFINEMENT_TABLE: ClassVar[dict[S3Operation, tuple[_RefinementRule, ...]]] = {
        S3Operation.PUT_OBJECT: (
            _RefinementRule(S3Operation.PUT_OBJECT_TAGGING, query_key="tagging"),
            _RefinementRule(S3Operation.UPLOAD_PART, query_key="partNumber", extra_query_key="uploadId"),
            _RefinementRule(S3Operation.COPY_OBJECT, header_key="x-amz-copy-source"),
        ),
        S3Operation.POST_OBJECT: (
            _RefinementRule(S3Operation.CREATE_MULTIPART_UPLOAD, query_key="uploads"),
            _RefinementRule(S3Operation.COMPLETE_MULTIPART_UPLOAD, query_key="uploadId"),
        ),
        S3Operation.GET_OBJECT: (_RefinementRule(S3Operation.GET_OBJECT_TAGGING, query_key="tagging"),),
        S3Operation.DELETE_OBJECT: (
            _RefinementRule(S3Operation.DELETE_OBJECT_TAGGING, query_key="tagging"),
            _RefinementRule(S3Operation.ABORT_MULTIPART_UPLOAD, query_key="uploadId"),
        ),
        S3Operation.LIST_OBJECTS_V2: (
            _RefinementRule(S3Operation.GET_BUCKET_LIFECYCLE, query_key="lifecycle"),
            _RefinementRule(S3Operation.GET_BUCKET_POLICY, query_key="policy"),
        ),
        S3Operation.CREATE_BUCKET: (
            _RefinementRule(S3Operation.PUT_BUCKET_LIFECYCLE, query_key="lifecycle"),
            _RefinementRule(S3Operation.PUT_BUCKET_POLICY, query_key="policy"),
        ),
        S3Operation.DELETE_BUCKET: (
            _RefinementRule(S3Operation.DELETE_BUCKET_LIFECYCLE, query_key="lifecycle"),
            _RefinementRule(S3Operation.DELETE_BUCKET_POLICY, query_key="policy"),
        ),
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
        """Refine base operation using the declarative refinement table."""
        rules = self._REFINEMENT_TABLE.get(base_op)
        if rules:
            for rule in rules:
                if rule.matches(query, headers):
                    return rule.refined_op

        # Validation: POST at object depth requires ?uploads or ?uploadId
        if base_op == S3Operation.POST_OBJECT:
            raise ValueError(f"Cannot classify POST request without uploads or uploadId: {path}")

        # Validation: POST at bucket depth requires ?delete
        if base_op == S3Operation.DELETE_OBJECTS and "delete" not in query:
            raise ValueError(f"Cannot classify POST request without 'delete' query param: {path}")

        # GET /bucket without ?list-type=2 → ListObjects V1
        if base_op == S3Operation.LIST_OBJECTS_V2 and query.get("list-type") != "2":
            return S3Operation.LIST_OBJECTS

        return base_op
