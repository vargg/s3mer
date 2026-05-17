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

    # Bucket Lifecycle operations
    GET_BUCKET_LIFECYCLE = "get_bucket_lifecycle_configuration"
    PUT_BUCKET_LIFECYCLE = "put_bucket_lifecycle_configuration"
    DELETE_BUCKET_LIFECYCLE = "delete_bucket_lifecycle_configuration"

    # Bucket Policy operations
    GET_BUCKET_POLICY = "get_bucket_policy"
    PUT_BUCKET_POLICY = "put_bucket_policy"
    DELETE_BUCKET_POLICY = "delete_bucket_policy"

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
    def operation_type(self) -> str:
        """Whether this operation reads or writes."""
        from s3mer.routing.registry import OperationType, registry  # noqa: PLC0415

        meta = registry.get(self)
        if meta and meta.operation_type == OperationType.WRITE:
            return "write"
        return "read"

    @property
    def is_read(self) -> bool:
        return self.operation_type == "read"

    @property
    def is_write(self) -> bool:
        return self.operation_type == "write"

    @property
    def is_object_operation(self) -> bool:
        """True if this operation targets an object (vs. a bucket)."""
        from s3mer.routing.registry import registry  # noqa: PLC0415

        meta = registry.get(self)
        return meta.is_object_op if meta else True
