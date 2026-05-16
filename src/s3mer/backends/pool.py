"""Backend pool — manages all configured S3 backend clients."""

from s3m.backends.client import S3BackendClient
from s3m.common.logging import get_logger
from s3m.common.metrics import MetricsTracker
from s3m.config.settings import BackendConfig

logger = get_logger(__name__)


class BackendPool:
    """
    Manages the lifecycle and access patterns for all S3 backend clients.

    Provides access to the primary backend, secondary backends,
    and iteration over all backends sorted by read priority.
    """

    def __init__(self, configs: list[BackendConfig], metrics: MetricsTracker) -> None:
        self._clients: dict[str, S3BackendClient] = {}
        self._primary: S3BackendClient | None = None

        for cfg in configs:
            client = S3BackendClient(cfg, metrics)
            self._clients[cfg.name] = client
            if cfg.is_primary:
                self._primary = client

    async def start(self) -> None:
        """Start all backend clients."""
        for client in self._clients.values():
            await client.start()
        logger.info(
            "Backend pool started",
            backends=list(self._clients.keys()),
            primary=self._primary.name if self._primary else None,
        )

    async def close(self) -> None:
        """Close all backend clients."""
        for client in self._clients.values():
            await client.close()
        logger.info("Backend pool closed")

    @property
    def primary(self) -> S3BackendClient:
        """Get the primary backend client."""
        if self._primary is None:
            msg = "No primary backend configured"
            raise RuntimeError(msg)
        return self._primary

    def get(self, name: str) -> S3BackendClient:
        """Get a backend client by name."""
        client = self._clients.get(name)
        if client is None:
            raise KeyError(f"Unknown backend: {name}. Available: {list(self._clients.keys())}")
        return client

    def get_secondaries(self) -> list[S3BackendClient]:
        """Get all non-primary backends."""
        return [c for c in self._clients.values() if not c.is_primary]

    def get_write_candidates(self) -> list[S3BackendClient]:
        """
        Get all backends available for writing, in order of preference.
        Primary first, then secondaries sorted by priority.
        """
        candidates = [self.primary]
        secondaries = sorted(self.get_secondaries(), key=lambda c: c.priority)
        candidates.extend(secondaries)
        return candidates

    def all_by_priority(self) -> list[S3BackendClient]:
        """Get all backends sorted by read priority (lowest first)."""
        return sorted(self._clients.values(), key=lambda c: c.priority)

    @property
    def all_clients(self) -> list[S3BackendClient]:
        """Get all backend clients (unordered)."""
        return list(self._clients.values())
