"""S3 operation enum with metadata for routing decisions."""

from __future__ import annotations

from enum import StrEnum


class OperationType(StrEnum):
    """Whether an S3 operation is a read or write."""

    READ = "read"
    WRITE = "write"


class S3Operation(StrEnum):
    """
    Supported S3 API operations.

    Each variant maps to the boto3/aiobotocore method name and carries
    metadata about whether it's a read or write operation.
    """

    # Bucket operations
    CREATE_BUCKET = "create_bucket"
    DELETE_BUCKET = "delete_bucket"
    HEAD_BUCKET = "head_bucket"
    LIST_BUCKETS = "list_buckets"

    # Object operations
    PUT_OBJECT = "put_object"
    GET_OBJECT = "get_object"
    DELETE_OBJECT = "delete_object"
    HEAD_OBJECT = "head_object"

    @property
    def boto_method(self) -> str:
        """The aiobotocore client method name for this operation."""
        return self.value

    @property
    def operation_type(self) -> OperationType:
        """Whether this operation reads or writes."""
        return _OPERATION_TYPES[self]

    @property
    def is_read(self) -> bool:
        return self.operation_type == OperationType.READ

    @property
    def is_write(self) -> bool:
        return self.operation_type == OperationType.WRITE

    @property
    def is_object_operation(self) -> bool:
        """True if this operation targets an object (vs. a bucket)."""
        return self in _OBJECT_OPERATIONS

    @property
    def is_bucket_operation(self) -> bool:
        """True if this operation targets a bucket."""
        return self not in _OBJECT_OPERATIONS


_OPERATION_TYPES: dict[S3Operation, OperationType] = {
    S3Operation.CREATE_BUCKET: OperationType.WRITE,
    S3Operation.DELETE_BUCKET: OperationType.WRITE,
    S3Operation.HEAD_BUCKET: OperationType.READ,
    S3Operation.LIST_BUCKETS: OperationType.READ,
    S3Operation.PUT_OBJECT: OperationType.WRITE,
    S3Operation.GET_OBJECT: OperationType.READ,
    S3Operation.DELETE_OBJECT: OperationType.WRITE,
    S3Operation.HEAD_OBJECT: OperationType.READ,
}

_OBJECT_OPERATIONS: frozenset[S3Operation] = frozenset(
    {
        S3Operation.PUT_OBJECT,
        S3Operation.GET_OBJECT,
        S3Operation.DELETE_OBJECT,
        S3Operation.HEAD_OBJECT,
    }
)
