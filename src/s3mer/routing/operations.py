"""S3 operation enum with metadata for routing decisions."""

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
    DELETE_OBJECTS = "delete_objects"
    LIST_OBJECTS = "list_objects"

    # Object operations
    PUT_OBJECT = "put_object"
    GET_OBJECT = "get_object"
    DELETE_OBJECT = "delete_object"
    HEAD_OBJECT = "head_object"
    POST_OBJECT = "post_object"
    COPY_OBJECT = "copy_object"

    # Tagging operations
    PUT_OBJECT_TAGGING = "put_object_tagging"
    GET_OBJECT_TAGGING = "get_object_tagging"
    DELETE_OBJECT_TAGGING = "delete_object_tagging"

    # Multipart operations
    CREATE_MULTIPART_UPLOAD = "create_multipart_upload"
    UPLOAD_PART = "upload_part"
    COMPLETE_MULTIPART_UPLOAD = "complete_multipart_upload"
    ABORT_MULTIPART_UPLOAD = "abort_multipart_upload"

    # Bucket object listing
    LIST_OBJECTS_V2 = "list_objects_v2"

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
    S3Operation.DELETE_OBJECTS: OperationType.WRITE,
    S3Operation.LIST_OBJECTS: OperationType.READ,
    S3Operation.PUT_OBJECT: OperationType.WRITE,
    S3Operation.GET_OBJECT: OperationType.READ,
    S3Operation.DELETE_OBJECT: OperationType.WRITE,
    S3Operation.HEAD_OBJECT: OperationType.READ,
    S3Operation.POST_OBJECT: OperationType.WRITE,
    S3Operation.COPY_OBJECT: OperationType.WRITE,
    S3Operation.PUT_OBJECT_TAGGING: OperationType.WRITE,
    S3Operation.GET_OBJECT_TAGGING: OperationType.READ,
    S3Operation.DELETE_OBJECT_TAGGING: OperationType.WRITE,
    S3Operation.CREATE_MULTIPART_UPLOAD: OperationType.WRITE,
    S3Operation.UPLOAD_PART: OperationType.WRITE,
    S3Operation.COMPLETE_MULTIPART_UPLOAD: OperationType.WRITE,
    S3Operation.ABORT_MULTIPART_UPLOAD: OperationType.WRITE,
    S3Operation.LIST_OBJECTS_V2: OperationType.READ,
}

_OBJECT_OPERATIONS: frozenset[S3Operation] = frozenset(
    {
        S3Operation.PUT_OBJECT,
        S3Operation.GET_OBJECT,
        S3Operation.DELETE_OBJECT,
        S3Operation.HEAD_OBJECT,
        S3Operation.POST_OBJECT,
        S3Operation.COPY_OBJECT,
        S3Operation.PUT_OBJECT_TAGGING,
        S3Operation.GET_OBJECT_TAGGING,
        S3Operation.DELETE_OBJECT_TAGGING,
        S3Operation.CREATE_MULTIPART_UPLOAD,
        S3Operation.UPLOAD_PART,
        S3Operation.COMPLETE_MULTIPART_UPLOAD,
        S3Operation.ABORT_MULTIPART_UPLOAD,
    },
)
