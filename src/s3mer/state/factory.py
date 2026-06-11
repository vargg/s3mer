"""Factories for optional external state backends."""

from s3mer.config.settings import Settings, WriteStrategyType
from s3mer.state.memory import MemoryMultipartSessionStore
from s3mer.state.protocol import MultipartSessionStore


def create_multipart_session_store(settings: Settings) -> MultipartSessionStore:
    """Create a multipart session store when distributed multi-sync is enabled."""
    if settings.write_strategy != WriteStrategyType.MULTI_SYNC_DISTRIBUTED:
        return MemoryMultipartSessionStore()

    from s3mer.state.valkey_store import ValkeyMultipartSessionStore  # noqa: PLC0415

    return ValkeyMultipartSessionStore(settings.valkey)


async def start_multipart_session_store(store: MultipartSessionStore) -> None:
    """Start stores that require external connections (Valkey)."""
    await store.start()


async def close_multipart_session_store(store: MultipartSessionStore) -> None:
    """Close external session store connections."""
    await store.close()
