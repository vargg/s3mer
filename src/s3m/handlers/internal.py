"""Internal service handlers for metrics and health checks."""

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from s3m.common.responses import ASGIResponse
from s3m.common.types import Receive, Scope, Send


async def metrics_handler(scope: Scope, receive: Receive, send: Send) -> None:
    """ASGI handler for /metrics endpoint."""
    data = generate_latest()
    response = ASGIResponse(
        content=data,
        status_code=200,
        media_type=CONTENT_TYPE_LATEST,
    )
    await response(scope, receive, send)


async def health_handler(scope: Scope, receive: Receive, send: Send) -> None:
    """ASGI handler for /health endpoint."""
    response = ASGIResponse(
        content=b'{"status":"ok"}',
        status_code=200,
        media_type="application/json",
    )
    await response(scope, receive, send)
