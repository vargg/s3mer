"""Unit tests for backend circuit breaker."""

import time

from s3mer.backends.circuit_breaker import BackendCircuitBreaker, CircuitState
from s3mer.common.metrics import NullMetricsTracker


def test_circuit_opens_after_threshold() -> None:
    metrics = NullMetricsTracker()
    cb = BackendCircuitBreaker("b1", metrics, failure_threshold=3, open_duration_seconds=60.0)

    cb.record_failure()
    cb.record_failure()
    assert cb.allow_request() is True

    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.allow_request() is False


def test_circuit_half_open_after_cooldown() -> None:
    metrics = NullMetricsTracker()
    cb = BackendCircuitBreaker("b1", metrics, failure_threshold=1, open_duration_seconds=0.01)

    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    time.sleep(0.02)
    assert cb.state == CircuitState.HALF_OPEN
    assert cb.allow_request() is True


def test_success_closes_circuit() -> None:
    metrics = NullMetricsTracker()
    cb = BackendCircuitBreaker("b1", metrics, failure_threshold=1, open_duration_seconds=0.01)
    cb.record_failure()
    time.sleep(0.02)
    assert cb.state == CircuitState.HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitState.CLOSED
