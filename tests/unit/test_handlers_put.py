"""Unit tests for PutObject handler error mapping."""

from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka.errors import KafkaConnectionError

from s3mer.common.metrics import NullMetricsTracker
from s3mer.common.responses import ASGIResponse
from s3mer.handlers.objects import handle_put_object
from s3mer.routing.operations import S3Operation
from s3mer.routing.registry import HandlerContext


@pytest.fixture
def ctx() -> HandlerContext:
    write_strategy = AsyncMock()
    write_strategy.execute = AsyncMock(side_effect=KafkaConnectionError("broker down"))
    return HandlerContext(
        operation=S3Operation.PUT_OBJECT,
        bucket="test-bucket",
        key="test-key",
        pool=MagicMock(),
        read_strategy=MagicMock(),
        write_strategy=write_strategy,
        metrics=NullMetricsTracker(),
        headers={"content-type": "text/plain"},
        query_string=b"",
        body=b"data",
        content_length=4,
    )


async def test_put_object_kafka_publish_failure_returns_non_2xx(ctx: HandlerContext) -> None:
    response = await handle_put_object(ctx)

    assert isinstance(response, ASGIResponse)
    assert response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
    assert b"ServiceUnavailable" in response.body
