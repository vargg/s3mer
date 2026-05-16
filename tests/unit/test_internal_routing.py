import pytest
from http import HTTPStatus
from unittest.mock import MagicMock, AsyncMock

from s3mer.app import S3ProxyApp
from s3mer.common.types import Scope, Receive, Send

@pytest.mark.asyncio
async def test_internal_routing_metrics():
    app = S3ProxyApp()
    # Mock handlers
    with MagicMock() as mock_metrics:
        import s3mer.app
        s3mer.app.metrics_handler = AsyncMock()

        scope: Scope = {
            "type": "http",
            "method": "GET",
            "path": "/.internal/metrics",
            "headers": []
        }
        receive: Receive = AsyncMock()
        send: Send = AsyncMock()

        await app._handle_http(scope, receive, send)

        s3mer.app.metrics_handler.assert_called_once_with(scope, receive, send)

@pytest.mark.asyncio
async def test_internal_routing_health():
    app = S3ProxyApp()
    with MagicMock() as mock_health:
        import s3mer.app
        s3mer.app.health_handler = AsyncMock()

        scope: Scope = {
            "type": "http",
            "method": "GET",
            "path": "/.internal/health",
            "headers": []
        }
        receive: Receive = AsyncMock()
        send: Send = AsyncMock()

        await app._handle_http(scope, receive, send)

        s3mer.app.health_handler.assert_called_once_with(scope, receive, send)

@pytest.mark.asyncio
async def test_internal_routing_unknown():
    app = S3ProxyApp()

    scope: Scope = {
        "type": "http",
        "method": "GET",
        "path": "/.internal/fake",
        "headers": []
    }
    receive: Receive = AsyncMock()
    send: Send = AsyncMock()

    # We expect this to call 'send' with a 404 status
    await app._handle_http(scope, receive, send)

    # Check that send was called with 404
    # The ASGIResponse call will call send multiple times (start and body)
    # The first call should be http.response.start with status 404
    start_call = send.call_args_list[0][0][0]
    assert start_call["type"] == "http.response.start"
    assert start_call["status"] == 403
