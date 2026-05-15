"""Prometheus metrics registry and ASGI handler."""

from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.utils import INF

from s3m.common.responses import ASGIResponse

HTTP_REQUESTS_TOTAL = Counter(
    "s3m_http_requests_total",
    "Total HTTP requests to the S3 proxy",
    ["method", "operation", "status"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "s3m_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "operation"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, INF),
)


async def metrics_handler(scope: dict, receive: Any, send: Any) -> None:
    """ASGI handler for /metrics endpoint."""
    data = generate_latest()
    response = ASGIResponse(
        content=data,
        status_code=200,
        media_type=CONTENT_TYPE_LATEST,
    )
    await response(scope, receive, send)


async def health_handler(scope: dict, receive: Any, send: Any) -> None:
    """ASGI handler for /health endpoint."""
    response = ASGIResponse(
        content=b'{"status":"ok"}',
        status_code=200,
        media_type="application/json",
    )
    await response(scope, receive, send)
