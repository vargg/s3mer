from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest.mock import ANY, AsyncMock, MagicMock

from s3mer.routing.http_handler import S3HTTPHandler

if TYPE_CHECKING:
    from s3mer.common.types import Receive, Scope, Send


def create_handler() -> S3HTTPHandler:
    return S3HTTPHandler(
        request_classifier=MagicMock(),
        dispatcher=AsyncMock(),
        metrics_tracker=MagicMock(),
    )


async def test_internal_routing_metrics() -> None:
    handler = create_handler()
    mock_metrics = AsyncMock()
    setattr(handler, "_internal_routes", {"GET": {"/.internal/metrics": mock_metrics}})  # noqa: B010

    scope: Scope = {"type": "http", "method": "GET", "path": "/.internal/metrics", "headers": []}
    receive: Receive = AsyncMock()
    send: Send = AsyncMock()

    await handler(scope, receive, send)

    mock_metrics.assert_called_once_with(scope, receive, ANY)


async def test_internal_routing_health() -> None:
    handler = create_handler()
    mock_health = AsyncMock()
    setattr(handler, "_internal_routes", {"GET": {"/.internal/health": mock_health}})  # noqa: B010

    scope: Scope = {"type": "http", "method": "GET", "path": "/.internal/health", "headers": []}
    receive: Receive = AsyncMock()
    send: Send = AsyncMock()

    await handler(scope, receive, send)

    mock_health.assert_called_once_with(scope, receive, ANY)


async def test_internal_routing_unknown() -> None:
    handler = create_handler()

    scope: Scope = {"type": "http", "method": "GET", "path": "/.internal/fake", "headers": []}
    receive: Receive = AsyncMock()
    send: Send = AsyncMock()

    # We expect this to call 'send' with a 403 status (Access Denied)
    await handler(scope, receive, send)

    # Check that send was called with 403
    # The ASGIResponse call will call send multiple times (start and body)
    # The first call should be http.response.start with status 403
    start_call = send.call_args_list[0][0][0]
    assert start_call["type"] == "http.response.start"
    assert start_call["status"] == HTTPStatus.FORBIDDEN
