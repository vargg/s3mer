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

    def record_replication_consumer_outcome(self, operation: str, target_backend: str, outcome: str) -> None:
        """Record a replication consumer outcome (success, skipped_*, etc.)."""
        ...

    def record_replication_retry(self, operation: str, target_backend: str) -> None:
        """Record one background replication retry attempt."""
        ...

    def record_replication_partition_paused(self, topic: str, partition: int) -> None:
        """Record a consumer partition paused for replication retry."""
        ...

    def record_replication_partition_resumed(self, topic: str, partition: int) -> None:
        """Record a consumer partition resumed after replication retry."""
        ...

    def set_replication_consumer_concurrency(self, concurrency: int) -> None:
        """Set the configured replication consumer concurrency gauge."""
        ...

    def set_replication_paused_partition(self, topic: str, partition: int, paused: bool) -> None:
        """Set whether a consumer partition is currently paused (1=paused, 0=active)."""
        ...

    def set_replication_background_retries_in_flight(self, mode: str, count: int) -> None:
        """Set the number of active background replication retry tasks."""
        ...

    def record_replication_dlq(self, reason: str) -> None:
        """Record a message published to the replication DLQ."""
        ...

    def set_backend_circuit_state(self, backend: str, state: str) -> None:
        """Set circuit breaker state for a backend (closed/open/half_open)."""
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
    def record_replication_consumer_outcome(self, operation: str, target_backend: str, outcome: str) -> None: ...
    def record_replication_retry(self, operation: str, target_backend: str) -> None: ...
    def record_replication_partition_paused(self, topic: str, partition: int) -> None: ...
    def record_replication_partition_resumed(self, topic: str, partition: int) -> None: ...
    def set_replication_consumer_concurrency(self, concurrency: int) -> None: ...
    def set_replication_paused_partition(self, topic: str, partition: int, paused: bool) -> None: ...
    def set_replication_background_retries_in_flight(self, mode: str, count: int) -> None: ...
    def record_replication_dlq(self, reason: str) -> None: ...
    def set_backend_circuit_state(self, backend: str, state: str) -> None: ...


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

_REPLICATION_CONSUMER_OUTCOMES_TOTAL = Counter(
    "s3mer_replication_consumer_outcomes_total",
    "Replication consumer outcomes per operation and target",
    ["operation", "target_backend", "outcome"],
)

_REPLICATION_RETRIES_TOTAL = Counter(
    "s3mer_replication_retries_total",
    "Background replication retry attempts",
    ["operation", "target_backend"],
)

_REPLICATION_PARTITION_PAUSED_TOTAL = Counter(
    "s3mer_replication_partition_paused_total",
    "Consumer partitions paused for replication retry",
    ["topic", "partition"],
)

_REPLICATION_PARTITION_RESUMED_TOTAL = Counter(
    "s3mer_replication_partition_resumed_total",
    "Consumer partitions resumed after replication retry",
    ["topic", "partition"],
)

_REPLICATION_CONSUMER_CONCURRENCY = Gauge(
    "s3mer_replication_consumer_concurrency",
    "Configured parallel replication message handlers per consumer",
)

_REPLICATION_PAUSED_PARTITIONS = Gauge(
    "s3mer_replication_paused_partitions",
    "Consumer partitions currently paused for replication retry",
    ["topic", "partition"],
)

_REPLICATION_BACKGROUND_RETRIES_IN_FLIGHT = Gauge(
    "s3mer_replication_background_retries_in_flight",
    "Active background replication retry tasks",
    ["mode"],
)

_REPLICATION_DLQ_TOTAL = Counter(
    "s3mer_replication_dlq_total",
    "Replication messages sent to the dead-letter queue",
    ["reason"],
)

_BACKEND_CIRCUIT_STATE = Gauge(
    "s3mer_backend_circuit_state",
    "Backend circuit breaker state (0=closed, 1=half_open, 2=open)",
    ["backend_name", "state"],
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

    def record_replication_consumer_outcome(self, operation: str, target_backend: str, outcome: str) -> None:
        _REPLICATION_CONSUMER_OUTCOMES_TOTAL.labels(
            operation=operation, target_backend=target_backend, outcome=outcome
        ).inc()

    def record_replication_retry(self, operation: str, target_backend: str) -> None:
        _REPLICATION_RETRIES_TOTAL.labels(operation=operation, target_backend=target_backend).inc()

    def record_replication_partition_paused(self, topic: str, partition: int) -> None:
        _REPLICATION_PARTITION_PAUSED_TOTAL.labels(topic=topic, partition=str(partition)).inc()

    def record_replication_partition_resumed(self, topic: str, partition: int) -> None:
        _REPLICATION_PARTITION_RESUMED_TOTAL.labels(topic=topic, partition=str(partition)).inc()

    def set_replication_consumer_concurrency(self, concurrency: int) -> None:
        _REPLICATION_CONSUMER_CONCURRENCY.set(concurrency)

    def set_replication_paused_partition(self, topic: str, partition: int, paused: bool) -> None:
        _REPLICATION_PAUSED_PARTITIONS.labels(topic=topic, partition=str(partition)).set(1 if paused else 0)

    def set_replication_background_retries_in_flight(self, mode: str, count: int) -> None:
        _REPLICATION_BACKGROUND_RETRIES_IN_FLIGHT.labels(mode=mode).set(count)

    def record_replication_dlq(self, reason: str) -> None:
        _REPLICATION_DLQ_TOTAL.labels(reason=reason).inc()

    def set_backend_circuit_state(self, backend: str, state: str) -> None:
        for label in ("closed", "half_open", "open"):
            _BACKEND_CIRCUIT_STATE.labels(backend_name=backend, state=label).set(1 if label == state else 0)


_global_tracker: PrometheusMetricsTracker | None = None


def get_tracker() -> MetricsTracker:
    """Get the global metrics tracker instance."""
    global _global_tracker  # noqa: PLW0603
    if _global_tracker is None:
        _global_tracker = PrometheusMetricsTracker()
    return _global_tracker
