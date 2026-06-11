"""S3 backend clients and operation strategies."""

from s3mer.backends.client import S3BackendClient
from s3mer.backends.multisync_strategies import (
    DistributedMultiSyncWriteStrategy,
    MultiSyncWriteStrategy,
    QuorumReplicationStrategy,
    SimpleMultiSyncWriteStrategy,
)
from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import OperationStrategy, ReadFallbackStrategy, WritePrimaryReplicationStrategy

__all__ = (
    "BackendPool",
    "DistributedMultiSyncWriteStrategy",
    "MultiSyncWriteStrategy",
    "OperationStrategy",
    "QuorumReplicationStrategy",
    "ReadFallbackStrategy",
    "S3BackendClient",
    "SimpleMultiSyncWriteStrategy",
    "WritePrimaryReplicationStrategy",
)
