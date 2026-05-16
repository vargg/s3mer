"""HTTP handlers for S3 operations."""

# Import handlers to register them in the HandlerRegistry
import s3mer.handlers.buckets as _buckets  # noqa: F401
import s3mer.handlers.objects as _objects  # noqa: F401
