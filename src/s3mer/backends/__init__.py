"""S3 backend clients and operation strategies."""

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import (
    OperationStrategy,
    ReadFallbackStrategy,
    WritePrimaryReplicationStrategy,
)

__all__ = [
    "BackendPool",
    "OperationStrategy",
    "ReadFallbackStrategy",
    "S3BackendClient",
    "WritePrimaryReplicationStrategy",
]
