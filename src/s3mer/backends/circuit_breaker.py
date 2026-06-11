"""Per-backend circuit breaker for fast failover on dead backends."""

import time
from enum import StrEnum

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker

logger = get_logger(__name__)


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BackendCircuitBreaker:
    """
    Simple circuit breaker: after N consecutive failures, skip the backend until cooldown.

    Success in half-open closes the circuit; failure reopens it.
    """

    def __init__(
        self,
        backend_name: str,
        metrics: MetricsTracker,
        *,
        failure_threshold: int = 3,
        open_duration_seconds: float = 30.0,
    ) -> None:
        self._backend_name = backend_name
        self._metrics = metrics
        self._failure_threshold = failure_threshold
        self._open_duration_seconds = open_duration_seconds
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> CircuitState:
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and time.monotonic() - self._opened_at >= self._open_duration_seconds
        ):
            self._state = CircuitState.HALF_OPEN
            self._record_state()
        return self._state

    def allow_request(self) -> bool:
        """Return False when the circuit is open (backend should be skipped)."""
        return self.state != CircuitState.OPEN

    def record_success(self) -> None:
        self._failure_count = 0
        if self._state != CircuitState.CLOSED:
            logger.info("Circuit breaker closed", backend=self._backend_name)
        self._state = CircuitState.CLOSED
        self._opened_at = None
        self._record_state()

    def record_failure(self) -> None:
        self._failure_count += 1
        if self._state == CircuitState.HALF_OPEN:
            self._open()
            return
        if self._failure_count >= self._failure_threshold:
            self._open()

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        logger.warning(
            "Circuit breaker opened",
            backend=self._backend_name,
            failure_count=self._failure_count,
            cooldown_seconds=self._open_duration_seconds,
        )
        self._record_state()

    def _record_state(self) -> None:
        self._metrics.set_backend_circuit_state(self._backend_name, self._state.value)
