"""Backend pool — manages all configured S3 backend clients."""

from s3mer.backends.circuit_breaker import BackendCircuitBreaker
from s3mer.backends.client import S3BackendClient
from s3mer.backends.memory_backend import MemoryS3BackendClient
from s3mer.backends.prober import DummyLatencyProber, LatencyProber, Prober
from s3mer.backends.types import BackendClient
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.config.settings import BackendConfig, BackendType, CircuitBreakerConfig

logger = get_logger(__name__)


class BackendPool:
    """
    Manages the lifecycle and access patterns for all S3 backend clients.

    Provides access to the primary backend, secondary backends,
    and iteration over all backends sorted by read priority.
    """

    def __init__(
        self,
        configs: dict[str, BackendConfig],
        metrics: MetricsTracker,
        probe_interval: float = 10.0,
        circuit_breaker: CircuitBreakerConfig | None = None,
    ) -> None:
        self._clients: dict[str, BackendClient] = {}
        self._primary: BackendClient | None = None
        self._circuit_breakers: dict[str, BackendCircuitBreaker] = {}
        cb_config = circuit_breaker or CircuitBreakerConfig()

        for name, cfg in configs.items():
            if cfg.backend_type == BackendType.MEMORY:
                client = MemoryS3BackendClient(name, cfg.is_primary, cfg.priority, metrics)
            else:
                client = S3BackendClient(name, cfg, metrics)
            self._clients[name] = client
            if cfg.is_primary:
                self._primary = client
            if cb_config.enabled:
                breaker = BackendCircuitBreaker(
                    name,
                    metrics,
                    failure_threshold=cb_config.failure_threshold,
                    open_duration_seconds=cb_config.open_duration_seconds,
                )
                self._circuit_breakers[name] = breaker
                client.set_circuit_breaker(breaker)

        prober_class = LatencyProber if probe_interval > 0 else DummyLatencyProber
        self._prober: Prober = prober_class(list(self._clients.values()), probe_interval)

    async def start(self) -> None:
        """Start all backend clients and initiate background latency probing."""
        for client in self._clients.values():
            await client.start()

        self._prober.start()

        logger.info(
            "Backend pool started",
            backends=list(self._clients.keys()),
            primary=self._primary.name if self._primary else None,
        )

    async def close(self) -> None:
        """Close all backend clients and stop the background prober."""
        await self._prober.close()

        for client in self._clients.values():
            await client.close()
        logger.info("Backend pool closed")

    @property
    def primary(self) -> BackendClient:
        """Get the primary backend client."""
        if self._primary is None:
            raise RuntimeError("No primary backend configured")
        return self._primary

    def get(self, name: str) -> BackendClient:
        """Get a backend client by name."""
        client = self._clients.get(name)
        if client is None:
            raise KeyError(f"Unknown backend: {name}. Available: {list(self._clients.keys())}")
        return client

    def get_secondaries(self) -> list[BackendClient]:
        """Get all non-primary backends."""
        return [c for c in self._clients.values() if not c.is_primary]

    def _filter_available(self, clients: list[BackendClient]) -> list[BackendClient]:
        return [
            c
            for c in clients
            if self._circuit_breakers.get(c.name, None) is None or self._circuit_breakers[c.name].allow_request()
        ]

    def get_write_candidates(self) -> list[BackendClient]:
        """
        Get all backends available for writing, in order of preference.
        Primary first, then secondaries sorted by priority.
        """
        candidates = [self.primary]
        secondaries = sorted(self.get_secondaries(), key=lambda c: c.priority)
        candidates.extend(secondaries)
        return self._filter_available(candidates)

    def all_by_latency(self) -> list[BackendClient]:
        """
        Get all backends for reading.

        Primary backend is always returned first to guarantee read-after-write consistency,
        followed by all secondary backends sorted by latency (lowest first, falling back to priority).
        """
        secondaries = sorted(self.get_secondaries(), key=lambda c: (c.last_latency, c.priority))
        return self._filter_available([self.primary, *secondaries])

    @property
    def all_clients(self) -> list[BackendClient]:
        """Get all backend clients (unordered)."""
        return list(self._clients.values())
