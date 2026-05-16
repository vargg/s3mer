from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest

from s3mer.app import S3ProxyApp

if TYPE_CHECKING:
    from s3mer.common.types import Receive, Scope, Send


@pytest.mark.asyncio
async def test_internal_routing_metrics() -> None:
    app = S3ProxyApp()
    # Mock handlers
    with patch("s3mer.app.metrics_handler", new_callable=AsyncMock) as mock_metrics:
        scope: Scope = {"type": "http", "method": "GET", "path": "/.internal/metrics", "headers": []}
        receive: Receive = AsyncMock()
        send: Send = AsyncMock()

        # Accessing private member for testing purposes
        await app._handle_http(scope, receive, send)

        mock_metrics.assert_called_once_with(scope, receive, send)


@pytest.mark.asyncio
async def test_internal_routing_health() -> None:
    app = S3ProxyApp()
    with patch("s3mer.app.health_handler", new_callable=AsyncMock) as mock_health:
        scope: Scope = {"type": "http", "method": "GET", "path": "/.internal/health", "headers": []}
        receive: Receive = AsyncMock()
        send: Send = AsyncMock()

        await app._handle_http(scope, receive, send)

        mock_health.assert_called_once_with(scope, receive, send)


@pytest.mark.asyncio
async def test_internal_routing_unknown() -> None:
    app = S3ProxyApp()

    scope: Scope = {"type": "http", "method": "GET", "path": "/.internal/fake", "headers": []}
    receive: Receive = AsyncMock()
    send: Send = AsyncMock()

    # We expect this to call 'send' with a 403 status (Access Denied)
    await app._handle_http(scope, receive, send)

    # Check that send was called with 403
    # The ASGIResponse call will call send multiple times (start and body)
    # The first call should be http.response.start with status 403
    start_call = send.call_args_list[0][0][0]
    assert start_call["type"] == "http.response.start"
    assert start_call["status"] == HTTPStatus.FORBIDDEN
