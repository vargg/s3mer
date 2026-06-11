"""Factory for write strategy selection."""

from typing import Any

from s3mer.backends.multisync_strategies import (
    DistributedMultiSyncWriteStrategy,
    QuorumReplicationStrategy,
    SimpleMultiSyncWriteStrategy,
)
from s3mer.backends.strategies import WritePrimaryReplicationStrategy
from s3mer.common.metrics import MetricsTracker
from s3mer.common.streaming import StreamConfig
from s3mer.config.settings import Settings, WriteStrategyType
from s3mer.kafka.manager import BaseReplicationManager
from s3mer.state.protocol import MultipartSessionStore


def build_write_strategy(
    settings: Settings,
    replication_manager: BaseReplicationManager,
    metrics: MetricsTracker,
    stream_config: StreamConfig,
    session_store: MultipartSessionStore,
) -> Any:
    """Construct the configured write strategy."""
    sync_kwargs = {
        "metrics": metrics,
        "stream_config": stream_config,
        "sync_backend_names": settings.sync_backends or None,
        "sync_quorum": settings.sync_quorum,
        "response_backend": settings.response_backend,
    }

    match settings.write_strategy:
        case WriteStrategyType.PRIMARY_REPLICATION:
            return WritePrimaryReplicationStrategy(replication_manager, metrics, stream_config)
        case WriteStrategyType.MULTI_SYNC | WriteStrategyType.MULTI_SYNC_SIMPLE:
            return SimpleMultiSyncWriteStrategy(**sync_kwargs)
        case WriteStrategyType.QUORUM_REPLICATION:
            return QuorumReplicationStrategy(replication_manager, **sync_kwargs)
        case WriteStrategyType.MULTI_SYNC_DISTRIBUTED:
            return DistributedMultiSyncWriteStrategy(replication_manager, session_store, **sync_kwargs)
        case _:
            raise ValueError(f"Unsupported write strategy: {settings.write_strategy}")
