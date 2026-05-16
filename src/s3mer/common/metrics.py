"""Prometheus implementation of the MetricsTracker interface."""

from typing import Protocol, runtime_checkable

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.utils import INF


@runtime_checkable
class MetricsTracker(Protocol):
    """
    Protocol defining all required instrumentation methods for the S3 proxy.

    This allows the application to remain decoupled from the underlying
    monitoring library (e.g. Prometheus, OpenTelemetry).
    """

    def record_request(self, method: str, operation: str, status: int, duration: float) -> None:
        """Record an incoming HTTP request."""
        ...

    def record_data_transfer(self, direction: str, operation: str, bytes_count: int) -> None:
        """Record ingress/egress data transfer."""
        ...

    def record_replication_task(self, operation: str, target: str) -> None:
        """Record a scheduled replication task."""
        ...

    def record_replication_fanout(self, operation: str, count: int) -> None:
        """Record the fan-out factor of a multi-key operation."""
        ...

    def record_backend_status(self, backend: str, is_up: bool) -> None:
        """Record the health status of a backend."""
        ...

    def record_backend_request(self, backend: str, operation: str, status: str, duration: float) -> None:
        """Record a request made to an underlying backend."""
        ...

    def record_active_stream_readers(self, count_delta: int) -> None:
        """Record the change in active BufferedStreamReader instances."""
        ...


class NullMetricsTracker:
    """No-op implementation of MetricsTracker for testing or disabled monitoring."""

    def record_request(self, method: str, operation: str, status: int, duration: float) -> None: ...
    def record_data_transfer(self, direction: str, operation: str, bytes_count: int) -> None: ...
    def record_replication_task(self, operation: str, target: str) -> None: ...
    def record_replication_fanout(self, operation: str, count: int) -> None: ...
    def record_backend_status(self, backend: str, is_up: bool) -> None: ...
    def record_backend_request(self, backend: str, operation: str, status: str, duration: float) -> None: ...
    def record_active_stream_readers(self, count_delta: int) -> None: ...


# --- Internal Prometheus Primitives ---

_HTTP_REQUESTS_TOTAL = Counter(
    "s3mer_http_requests_total",
    "Total HTTP requests to the S3 proxy",
    ["method", "operation", "status"],
)

_HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "s3mer_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "operation"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, INF),
)

_DATA_TRANSFER_BYTES_TOTAL = Counter(
    "s3mer_data_transfer_bytes_total",
    "Total bytes transferred through the proxy",
    ["direction", "operation"],
)

_REPLICATION_TASKS_TOTAL = Counter(
    "s3mer_replication_tasks_total",
    "Total replication tasks scheduled",
    ["operation", "target_backend"],
)

_REPLICATION_FANOUT_FACTOR = Histogram(
    "s3mer_replication_fanout_factor",
    "Number of replication messages generated per request",
    ["operation"],
    buckets=(1, 2, 5, 10, 20, 50, 100),
)

_BACKEND_STATUS = Gauge(
    "s3mer_backend_status",
    "Health status of underlying backends (1=UP, 0=DOWN)",
    ["backend_name"],
)

_BACKEND_REQUEST_DURATION_SECONDS = Histogram(
    "s3mer_backend_request_duration_seconds",
    "Latency of requests to underlying backends",
    ["backend_name", "operation", "status"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, INF),
)

_ACTIVE_STREAM_READERS = Gauge(
    "s3mer_active_stream_readers",
    "Number of active BufferedStreamReader instances",
)


class PrometheusMetricsTracker(MetricsTracker):
    """Prometheus implementation of metrics tracking."""

    def record_request(self, method: str, operation: str, status: int, duration: float) -> None:
        _HTTP_REQUESTS_TOTAL.labels(method=method, operation=operation, status=status).inc()
        _HTTP_REQUEST_DURATION_SECONDS.labels(method=method, operation=operation).observe(duration)

    def record_data_transfer(self, direction: str, operation: str, bytes_count: int) -> None:
        _DATA_TRANSFER_BYTES_TOTAL.labels(direction=direction, operation=operation).inc(bytes_count)

    def record_replication_task(self, operation: str, target: str) -> None:
        _REPLICATION_TASKS_TOTAL.labels(operation=operation, target_backend=target).inc()

    def record_replication_fanout(self, operation: str, count: int) -> None:
        _REPLICATION_FANOUT_FACTOR.labels(operation=operation).observe(count)

    def record_backend_status(self, backend: str, is_up: bool) -> None:
        _BACKEND_STATUS.labels(backend_name=backend).set(1 if is_up else 0)

    def record_backend_request(self, backend: str, operation: str, status: str, duration: float) -> None:
        _BACKEND_REQUEST_DURATION_SECONDS.labels(backend_name=backend, operation=operation, status=status).observe(
            duration
        )

    def record_active_stream_readers(self, count_delta: int) -> None:
        if count_delta > 0:
            _ACTIVE_STREAM_READERS.inc(count_delta)
        elif count_delta < 0:
            _ACTIVE_STREAM_READERS.dec(abs(count_delta))


# --- Singleton Instance ---
# While we prefer injection, having a default singleton simplifies some migrations
# and allows for easier access in top-level app construction.
_global_tracker = PrometheusMetricsTracker()


def get_tracker() -> MetricsTracker:
    """Get the global metrics tracker instance."""
    return _global_tracker
