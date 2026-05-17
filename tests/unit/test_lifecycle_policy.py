import dataclasses
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import ReadFallbackStrategy, WritePrimaryReplicationStrategy
from s3mer.common.metrics import MetricsTracker
from s3mer.common.responses import ASGIResponse
from s3mer.handlers.buckets import (
    handle_delete_bucket_lifecycle,
    handle_delete_bucket_policy,
    handle_get_bucket_lifecycle,
    handle_get_bucket_policy,
    handle_put_bucket_lifecycle,
    handle_put_bucket_policy,
)
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.subscribers import _replicate_operation
from s3mer.routing.classifier import RequestClassifier, S3Request
from s3mer.routing.operations import S3Operation
from s3mer.routing.registry import HandlerContext

# --- 1. CLASSIFIER TESTS ---


class TestLifecyclePolicyClassifier:
    """Test HTTP request classification for lifecycle and policy operations."""

    @pytest.fixture
    def classifier(self) -> RequestClassifier:
        return RequestClassifier()

    def test_get_bucket_lifecycle(self, classifier: RequestClassifier) -> None:
        result = classifier.classify("GET", "/my-bucket", b"lifecycle=")
        assert result == S3Request(
            operation=S3Operation.GET_BUCKET_LIFECYCLE,
            bucket="my-bucket",
        )

    def test_put_bucket_lifecycle(self, classifier: RequestClassifier) -> None:
        result = classifier.classify("PUT", "/my-bucket", b"lifecycle=")
        assert result == S3Request(
            operation=S3Operation.PUT_BUCKET_LIFECYCLE,
            bucket="my-bucket",
        )

    def test_delete_bucket_lifecycle(self, classifier: RequestClassifier) -> None:
        result = classifier.classify("DELETE", "/my-bucket", b"lifecycle=")
        assert result == S3Request(
            operation=S3Operation.DELETE_BUCKET_LIFECYCLE,
            bucket="my-bucket",
        )

    def test_get_bucket_policy(self, classifier: RequestClassifier) -> None:
        result = classifier.classify("GET", "/my-bucket", b"policy=")
        assert result == S3Request(
            operation=S3Operation.GET_BUCKET_POLICY,
            bucket="my-bucket",
        )

    def test_put_bucket_policy(self, classifier: RequestClassifier) -> None:
        result = classifier.classify("PUT", "/my-bucket", b"policy=")
        assert result == S3Request(
            operation=S3Operation.PUT_BUCKET_POLICY,
            bucket="my-bucket",
        )

    def test_delete_bucket_policy(self, classifier: RequestClassifier) -> None:
        result = classifier.classify("DELETE", "/my-bucket", b"policy=")
        assert result == S3Request(
            operation=S3Operation.DELETE_BUCKET_POLICY,
            bucket="my-bucket",
        )


# --- 2. HANDLER TESTS ---


@pytest.mark.asyncio
class TestLifecyclePolicyHandlers:
    """Test S3 HTTP handlers for lifecycle and policy operations."""

    @pytest.fixture
    def base_ctx(self) -> HandlerContext:
        return HandlerContext(
            operation=S3Operation.GET_BUCKET_LIFECYCLE,
            bucket="test-bucket",
            key=None,
            pool=MagicMock(spec=BackendPool),
            read_strategy=AsyncMock(spec=ReadFallbackStrategy),
            write_strategy=AsyncMock(spec=WritePrimaryReplicationStrategy),
            metrics=MagicMock(spec=MetricsTracker),
            headers={},
            query_string=b"",
            body=None,
            content_length=None,
        )

    async def test_handle_get_bucket_lifecycle(self, base_ctx: HandlerContext) -> None:
        rules = {
            "Rules": [
                {
                    "ID": "rule1",
                    "Prefix": "tmp/",
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        }
        cast("Any", base_ctx.read_strategy).execute.return_value = rules

        response = await handle_get_bucket_lifecycle(base_ctx)
        assert isinstance(response, ASGIResponse)
        assert response.status_code == 200  # noqa: PLR2004
        assert b"<ID>rule1</ID>" in response.body
        assert b"<Days>30</Days>" in response.body

    async def test_handle_put_bucket_lifecycle(self, base_ctx: HandlerContext) -> None:
        xml_body = (
            b"<LifecycleConfiguration>"
            b"  <Rule>"
            b"    <ID>rule1</ID>"
            b"    <Prefix>tmp/</Prefix>"
            b"    <Status>Enabled</Status>"
            b"    <Expiration>"
            b"      <Days>30</Days>"
            b"    </Expiration>"
            b"  </Rule>"
            b"</LifecycleConfiguration>"
        )
        ctx = dataclasses.replace(base_ctx, body=xml_body)

        response = await handle_put_bucket_lifecycle(ctx)
        assert isinstance(response, ASGIResponse)
        assert response.status_code == 200  # noqa: PLR2004

        cast("Any", base_ctx.write_strategy).execute.assert_called_once_with(
            S3Operation.PUT_BUCKET_LIFECYCLE,
            base_ctx.pool,
            {
                "Bucket": "test-bucket",
                "LifecycleConfiguration": {
                    "Rules": [
                        {
                            "ID": "rule1",
                            "Prefix": "tmp/",
                            "Status": "Enabled",
                            "Expiration": {"Days": 30},
                        }
                    ]
                },
            },
        )

    async def test_handle_delete_bucket_lifecycle(self, base_ctx: HandlerContext) -> None:
        response = await handle_delete_bucket_lifecycle(base_ctx)
        assert isinstance(response, ASGIResponse)
        assert response.status_code == 204  # noqa: PLR2004

        cast("Any", base_ctx.write_strategy).execute.assert_called_once_with(
            S3Operation.DELETE_BUCKET_LIFECYCLE,
            base_ctx.pool,
            {"Bucket": "test-bucket"},
        )

    async def test_handle_get_bucket_policy(self, base_ctx: HandlerContext) -> None:
        policy_str = '{"Statement": []}'
        cast("Any", base_ctx.read_strategy).execute.return_value = {"Policy": policy_str}

        response = await handle_get_bucket_policy(base_ctx)
        assert isinstance(response, ASGIResponse)
        assert response.status_code == 200  # noqa: PLR2004
        assert response.body == policy_str.encode()
        assert response.extra_headers.get("Content-Type") == "application/json"

    async def test_handle_put_bucket_policy(self, base_ctx: HandlerContext) -> None:
        policy_str = '{"Statement": []}'
        ctx = dataclasses.replace(base_ctx, body=policy_str.encode())

        response = await handle_put_bucket_policy(ctx)
        assert isinstance(response, ASGIResponse)
        assert response.status_code == 200  # noqa: PLR2004

        cast("Any", base_ctx.write_strategy).execute.assert_called_once_with(
            S3Operation.PUT_BUCKET_POLICY,
            base_ctx.pool,
            {
                "Bucket": "test-bucket",
                "Policy": policy_str,
            },
        )

    async def test_handle_delete_bucket_policy(self, base_ctx: HandlerContext) -> None:
        response = await handle_delete_bucket_policy(base_ctx)
        assert isinstance(response, ASGIResponse)
        assert response.status_code == 204  # noqa: PLR2004

        cast("Any", base_ctx.write_strategy).execute.assert_called_once_with(
            S3Operation.DELETE_BUCKET_POLICY,
            base_ctx.pool,
            {"Bucket": "test-bucket"},
        )


# --- 3. REPLICATION WORKER TESTS ---


@pytest.mark.asyncio
class TestLifecyclePolicyReplication:
    """Test background worker replication of bucket lifecycle and policy configurations."""

    @pytest.fixture
    def source(self) -> MagicMock:
        client = MagicMock(spec=S3BackendClient)
        client.name = "primary"
        client.execute = AsyncMock()
        return client

    @pytest.fixture
    def target(self) -> MagicMock:
        client = MagicMock(spec=S3BackendClient)
        client.name = "secondary"
        client.execute = AsyncMock()
        return client

    async def test_replicate_put_bucket_lifecycle(self, source: MagicMock, target: MagicMock) -> None:
        msg = ReplicationMessage(
            message_id="msg-1",
            operation=S3Operation.PUT_BUCKET_LIFECYCLE.value,
            bucket="test-bucket",
            key=None,
            source_backend="primary",
            target_backends=["secondary"],
            metadata={},
        )

        rules = {
            "Rules": [
                {
                    "ID": "rule1",
                    "Prefix": "tmp/",
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        }
        source.execute.return_value = rules

        await _replicate_operation(S3Operation.PUT_BUCKET_LIFECYCLE, msg, source, target)

        source.execute.assert_called_once_with(
            S3Operation.GET_BUCKET_LIFECYCLE,
            {"Bucket": "test-bucket"},
        )

        target.execute.assert_called_once_with(
            S3Operation.PUT_BUCKET_LIFECYCLE,
            {
                "Bucket": "test-bucket",
                "LifecycleConfiguration": rules,
            },
        )

    async def test_replicate_delete_bucket_lifecycle(self, source: MagicMock, target: MagicMock) -> None:
        msg = ReplicationMessage(
            message_id="msg-2",
            operation=S3Operation.DELETE_BUCKET_LIFECYCLE.value,
            bucket="test-bucket",
            key=None,
            source_backend="primary",
            target_backends=["secondary"],
            metadata={},
        )

        await _replicate_operation(S3Operation.DELETE_BUCKET_LIFECYCLE, msg, source, target)

        target.execute.assert_called_once_with(
            S3Operation.DELETE_BUCKET_LIFECYCLE,
            {"Bucket": "test-bucket"},
        )

    async def test_replicate_put_bucket_policy(self, source: MagicMock, target: MagicMock) -> None:
        msg = ReplicationMessage(
            message_id="msg-3",
            operation=S3Operation.PUT_BUCKET_POLICY.value,
            bucket="test-bucket",
            key=None,
            source_backend="primary",
            target_backends=["secondary"],
            metadata={},
        )

        policy_str = '{"Statement": []}'
        source.execute.return_value = {"Policy": policy_str}

        await _replicate_operation(S3Operation.PUT_BUCKET_POLICY, msg, source, target)

        source.execute.assert_called_once_with(
            S3Operation.GET_BUCKET_POLICY,
            {"Bucket": "test-bucket"},
        )

        target.execute.assert_called_once_with(
            S3Operation.PUT_BUCKET_POLICY,
            {
                "Bucket": "test-bucket",
                "Policy": policy_str,
            },
        )

    async def test_replicate_delete_bucket_policy(self, source: MagicMock, target: MagicMock) -> None:
        msg = ReplicationMessage(
            message_id="msg-4",
            operation=S3Operation.DELETE_BUCKET_POLICY.value,
            bucket="test-bucket",
            key=None,
            source_backend="primary",
            target_backends=["secondary"],
            metadata={},
        )

        await _replicate_operation(S3Operation.DELETE_BUCKET_POLICY, msg, source, target)

        target.execute.assert_called_once_with(
            S3Operation.DELETE_BUCKET_POLICY,
            {"Bucket": "test-bucket"},
        )
