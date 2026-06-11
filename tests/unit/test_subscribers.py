# ruff: noqa: PLR2004
"""Unit tests for Kafka replication subscribers and control loops."""

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka import TopicPartition
from botocore.exceptions import ClientError

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.common.metrics import NullMetricsTracker
from s3mer.config.settings import ReplicationMode
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.replication_executor import replicate_operation as _replicate_operation
from s3mer.kafka.retry_scheduler import schedule_global_retry as _schedule_global_retry
from s3mer.kafka.retry_scheduler import schedule_per_backend_retry as _schedule_per_backend_retry
from s3mer.kafka.subscribers import register_subscribers
from s3mer.kafka.subscribers_config import ReplicationRetryConfig
from s3mer.routing.operations import S3Operation


def make_client_error(code: str, http_status: int) -> ClientError:
    """Helper to construct a ClientError with specific code and HTTP status."""
    return ClientError(
        error_response={
            "Error": {"Code": code, "Message": "Test error"},
            "ResponseMetadata": {"HTTPStatusCode": http_status},
        },
        operation_name="PutObject",
    )


# ============================================================================
# Part 1: S3 Replication Dispatcher (_replicate_operation) Tests
# ============================================================================


class TestReplicateOperation:
    """Tests for the _replicate_operation S3 dispatcher."""

    @pytest.fixture
    def source(self) -> MagicMock:
        client = MagicMock(spec=S3BackendClient)
        client.execute = AsyncMock()
        return client

    @pytest.fixture
    def target(self) -> MagicMock:
        client = MagicMock(spec=S3BackendClient)
        client.execute = AsyncMock()
        return client

    async def test_put_object_success(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
            metadata={"ContentType": "text/plain", "ContentLength": "42"},
        )
        body_mock = AsyncMock()
        # Mock body async context manager
        body_mock.__aenter__.return_value = body_mock
        body_mock.__aexit__.return_value = None
        source.execute.return_value = {"Body": body_mock}

        await _replicate_operation(S3Operation.PUT_OBJECT, message, source, target)

        source.execute.assert_called_once_with(S3Operation.GET_OBJECT, {"Bucket": "b", "Key": "k"})
        target.execute.assert_called_once_with(
            S3Operation.PUT_OBJECT,
            {"Bucket": "b", "Key": "k", "Body": body_mock, "ContentType": "text/plain", "ContentLength": 42},
        )

    async def test_put_object_missing_content_length(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
            metadata={},  # No ContentLength
        )
        body_mock = AsyncMock()
        body_mock.__aenter__.return_value = body_mock
        source.execute.side_effect = [
            {"Body": body_mock},  # GET_OBJECT response
            {"ContentLength": 100},  # HEAD_OBJECT response
        ]

        await _replicate_operation(S3Operation.PUT_OBJECT, message, source, target)

        assert source.execute.call_count == 2
        source.execute.assert_any_call(S3Operation.GET_OBJECT, {"Bucket": "b", "Key": "k"})
        source.execute.assert_any_call(S3Operation.HEAD_OBJECT, {"Bucket": "b", "Key": "k"})
        target.execute.assert_called_once_with(
            S3Operation.PUT_OBJECT,
            {"Bucket": "b", "Key": "k", "Body": body_mock, "ContentLength": 100},
        )

    async def test_put_object_source_no_such_key_get(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
            metadata={},
        )
        source.execute.side_effect = make_client_error("NoSuchKey", 404)

        # Should log a warning and return gracefully (no exception raised)
        await _replicate_operation(S3Operation.PUT_OBJECT, message, source, target)

        target.execute.assert_not_called()

    async def test_put_object_source_no_such_key_head(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
            metadata={},
        )
        body_mock = AsyncMock()
        source.execute.side_effect = [
            {"Body": body_mock},  # GET_OBJECT response
            make_client_error("NoSuchKey", 404),  # HEAD_OBJECT fails
        ]

        # Should log a warning and return gracefully
        await _replicate_operation(S3Operation.PUT_OBJECT, message, source, target)

        target.execute.assert_not_called()

    async def test_bucket_operations(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="create_bucket",
            bucket="my-bucket",
            key=None,
            source_backend="p",
            target_backends=["s"],
        )
        await _replicate_operation(S3Operation.CREATE_BUCKET, message, source, target)
        target.execute.assert_called_once_with(S3Operation.CREATE_BUCKET, {"Bucket": "my-bucket"})

        target.execute.reset_mock()
        message.operation = "delete_bucket"
        await _replicate_operation(S3Operation.DELETE_BUCKET, message, source, target)
        target.execute.assert_called_once_with(S3Operation.DELETE_BUCKET, {"Bucket": "my-bucket"})

    async def test_delete_object(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="delete_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
        )
        await _replicate_operation(S3Operation.DELETE_OBJECT, message, source, target)
        target.execute.assert_called_once_with(S3Operation.DELETE_OBJECT, {"Bucket": "b", "Key": "k"})

    async def test_delete_object_no_such_key_is_idempotent(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="delete_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
        )
        target.execute.side_effect = make_client_error("NoSuchKey", 404)

        await _replicate_operation(S3Operation.DELETE_OBJECT, message, source, target)

        target.execute.assert_called_once()

    async def test_object_tagging_operations(self, source: MagicMock, target: MagicMock) -> None:
        # PUT Object Tagging
        message = ReplicationMessage(
            operation="put_object_tagging",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
        )
        source.execute.return_value = {"TagSet": [{"Key": "t1", "Value": "v1"}]}
        await _replicate_operation(S3Operation.PUT_OBJECT_TAGGING, message, source, target)

        source.execute.assert_called_once_with(S3Operation.GET_OBJECT_TAGGING, {"Bucket": "b", "Key": "k"})
        target.execute.assert_called_once_with(
            S3Operation.PUT_OBJECT_TAGGING,
            {"Bucket": "b", "Key": "k", "Tagging": {"TagSet": [{"Key": "t1", "Value": "v1"}]}},
        )

        # DELETE Object Tagging
        target.execute.reset_mock()
        await _replicate_operation(S3Operation.DELETE_OBJECT_TAGGING, message, source, target)
        target.execute.assert_called_once_with(S3Operation.DELETE_OBJECT_TAGGING, {"Bucket": "b", "Key": "k"})


# ============================================================================
# Part 2: Subscriber Pipelines & Retry Controls Tests
# ============================================================================


class TestSubscriberPipelines:
    """Tests for subscriber consumer pipelines, offset controls, and pausing."""

    @pytest.fixture
    def kafka_config(self) -> MagicMock:
        cfg = MagicMock()
        cfg.concurrency = 1
        cfg.replication_retry_delay = 0.01
        cfg.replication_max_retry_delay = 0.01
        cfg.replication_max_retries = 3
        cfg.replication_skip_if_etag_matches = False
        cfg.dlq_enabled = False
        cfg.dlq_topic_suffix = ".dlq"
        return cfg

    @pytest.fixture
    def pool(self) -> MagicMock:
        p = MagicMock(spec=BackendPool)
        p.get = MagicMock()
        p.get_secondaries = MagicMock()
        return p

    @pytest.fixture
    def mock_msg(self) -> MagicMock:
        record = MagicMock()
        record.partition = 0
        record.offset = 100
        msg = MagicMock()
        msg.raw_message = record
        return msg

    @pytest.fixture
    def mock_subscriber(self) -> MagicMock:
        sub = MagicMock()
        sub.consumer = MagicMock()
        sub.consumer.assignment.return_value = {TopicPartition("s3mer.replication", 0)}
        return sub

    @patch("s3mer.kafka.subscribers.replicate_operation", new_callable=AsyncMock)
    async def test_batch_replication_success(
        self,
        mock_replicate: AsyncMock,
        mock_msg: MagicMock,
        mock_subscriber: MagicMock,
        pool: MagicMock,
        kafka_config: MagicMock,
    ) -> None:
        mock_broker = MagicMock()
        mock_broker.subscriber.return_value = mock_subscriber

        # Register subscribers to initialize decorators
        register_subscribers(mock_broker, "s3mer.replication", pool, ReplicationMode.BATCH, kafka_config)

        # Prepare a valid PUT_OBJECT replication message
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="primary",
            target_backends=["sec1", "sec2"],
        )
        msg_raw = message.model_dump_json()

        # Execute subscriber handler
        handler = mock_subscriber.call_args_list[0][0][0]

        await handler(msg_raw, mock_msg)

        assert mock_replicate.call_count == 2
        mock_subscriber.consumer.pause.assert_not_called()

    @patch("s3mer.kafka.subscribers.replicate_operation", new_callable=AsyncMock)
    async def test_batch_replication_fail_action_skips_target(
        self,
        mock_replicate: AsyncMock,
        mock_msg: MagicMock,
        mock_subscriber: MagicMock,
        pool: MagicMock,
        kafka_config: MagicMock,
    ) -> None:
        mock_broker = MagicMock()
        mock_broker.subscriber.return_value = mock_subscriber

        register_subscribers(mock_broker, "s3mer.replication", pool, ReplicationMode.BATCH, kafka_config)
        handler = mock_subscriber.call_args_list[0][0][0]

        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="primary",
            target_backends=["sec1"],
        )
        msg_raw = message.model_dump_json()

        # Simulate permanent ClientError (AccessDenied 403)
        mock_replicate.side_effect = make_client_error("AccessDenied", 403)

        # Should execute successfully (returning None and skipping the target to commit offset)
        await handler(msg_raw, mock_msg)

        mock_subscriber.consumer.pause.assert_not_called()

    @patch("s3mer.kafka.subscribers.replicate_operation", new_callable=AsyncMock)
    async def test_batch_replication_retry_action_pauses_consumer(
        self,
        mock_replicate: AsyncMock,
        mock_msg: MagicMock,
        mock_subscriber: MagicMock,
        pool: MagicMock,
        kafka_config: MagicMock,
    ) -> None:
        mock_broker = MagicMock()
        mock_broker.subscriber.return_value = mock_subscriber

        register_subscribers(mock_broker, "s3mer.replication", pool, ReplicationMode.BATCH, kafka_config)
        handler = mock_subscriber.call_args_list[0][0][0]

        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="primary",
            target_backends=["sec1"],
        )
        msg_raw = message.model_dump_json()

        # Simulate transient error (ServiceUnavailable 503)
        mock_replicate.side_effect = make_client_error("SlowDown", 503)

        with pytest.raises(RuntimeError, match="Replication failed for targets"):
            await handler(msg_raw, mock_msg)

        mock_subscriber.consumer.pause.assert_called_once()

    @patch("s3mer.kafka.subscribers.replicate_operation", new_callable=AsyncMock)
    async def test_per_backend_replication_retry_action_pauses_partition(
        self,
        mock_replicate: AsyncMock,
        mock_msg: MagicMock,
        mock_subscriber: MagicMock,
        pool: MagicMock,
    ) -> None:
        mock_broker = MagicMock()
        mock_broker.subscriber.return_value = mock_subscriber

        secondary = MagicMock()
        secondary.name = "sec1"
        pool.get_secondaries.return_value = [secondary]

        # Register per-backend mode
        register_subscribers(mock_broker, "s3mer.replication", pool, ReplicationMode.PER_BACKEND)
        # Note: in per-backend mode, register_subscribers calls _register_per_backend_subscriber
        # Let's extract the per-backend decorated handler
        handler = mock_subscriber.call_args_list[0][0][0]

        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="primary",
            target_backends=["sec1"],
        )
        msg_raw = message.model_dump_json()

        # Transient connection timeout
        mock_replicate.side_effect = ConnectionError("timeout")

        with pytest.raises(RuntimeError, match="Replication failed for target"):
            await handler(msg_raw, mock_msg)

        mock_subscriber.consumer.pause.assert_called_once_with(TopicPartition("s3mer.replication.sec1", 0))


# ============================================================================
# Part 3: Background Retry Tasks (_schedule_global_retry & _schedule_per_backend_retry) Tests
# ============================================================================


class TestBackgroundRetryLoops:
    """Tests background retry loops, exponential backoff, seeks, and partition resumptions."""

    @pytest.fixture(autouse=True)
    def _restore_retry_config(self) -> Iterator[None]:
        original = ReplicationRetryConfig.max_retries
        ReplicationRetryConfig.max_retries = 10
        yield
        ReplicationRetryConfig.max_retries = original

    @pytest.fixture
    def metrics(self) -> NullMetricsTracker:
        return NullMetricsTracker()

    @pytest.fixture
    def mock_subscriber(self) -> MagicMock:
        sub = MagicMock()
        sub.consumer = MagicMock()
        return sub

    @pytest.fixture
    def source(self) -> MagicMock:
        mock = MagicMock(spec=S3BackendClient)
        mock.name = "p"
        return mock

    @pytest.fixture
    def target(self) -> MagicMock:
        mock = MagicMock(spec=S3BackendClient)
        mock.name = "sec1"
        return mock

    @pytest.fixture
    def pool(self, target: MagicMock) -> MagicMock:
        p = MagicMock(spec=BackendPool)
        p.get.return_value = target
        return p

    @patch("s3mer.kafka.retry_scheduler.asyncio.sleep", new_callable=AsyncMock)
    @patch("s3mer.kafka.retry_scheduler.replicate_operation", new_callable=AsyncMock)
    async def test_schedule_global_retry_success(
        self,
        mock_replicate: AsyncMock,
        mock_sleep: AsyncMock,
        mock_subscriber: MagicMock,
        source: MagicMock,
        pool: MagicMock,
        metrics: NullMetricsTracker,
    ) -> None:
        tp = TopicPartition("s3mer.replication", 0)
        assigned = {tp}
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["sec1"],
        )

        # 1st attempt fails, 2nd attempt succeeds
        mock_replicate.side_effect = [
            ConnectionError("transient"),  # Fails
            None,  # Succeeds
        ]

        await _schedule_global_retry(
            subscriber=mock_subscriber,
            failed_tp=tp,
            failed_offset=100,
            assigned_partitions=assigned,
            message=message,
            operation=S3Operation.PUT_OBJECT,
            source=source,
            failed_targets=["sec1"],
            pool=pool,
            metrics=metrics,
        )

        assert mock_sleep.call_count == 2
        mock_subscriber.consumer.seek.assert_called_once_with(tp, 100)
        mock_subscriber.consumer.resume.assert_called_once_with(tp)

    @patch("s3mer.kafka.retry_scheduler.asyncio.sleep", new_callable=AsyncMock)
    @patch("s3mer.kafka.retry_scheduler.replicate_operation", new_callable=AsyncMock)
    async def test_schedule_global_retry_permanent_failure_skips(
        self,
        mock_replicate: AsyncMock,
        mock_sleep: AsyncMock,
        mock_subscriber: MagicMock,
        source: MagicMock,
        pool: MagicMock,
        metrics: NullMetricsTracker,
    ) -> None:
        tp = TopicPartition("s3mer.replication", 0)
        assigned = {tp}
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["sec1"],
        )

        # Immediately fails with AccessDenied
        mock_replicate.side_effect = make_client_error("AccessDenied", 403)

        await _schedule_global_retry(
            subscriber=mock_subscriber,
            failed_tp=tp,
            failed_offset=100,
            assigned_partitions=assigned,
            message=message,
            operation=S3Operation.PUT_OBJECT,
            source=source,
            failed_targets=["sec1"],
            pool=pool,
            metrics=metrics,
        )

        # Since it's a FAIL action, the global retry loop should skip the target
        # and immediately terminate (because no targets remain failing), resuming partitions.
        assert mock_sleep.call_count == 1
        mock_subscriber.consumer.seek.assert_called_once_with(tp, 100)
        mock_subscriber.consumer.resume.assert_called_once_with(tp)

    @patch("s3mer.kafka.retry_scheduler.asyncio.sleep", new_callable=AsyncMock)
    @patch("s3mer.kafka.retry_scheduler.replicate_operation", new_callable=AsyncMock)
    async def test_schedule_per_backend_retry_success(
        self,
        mock_replicate: AsyncMock,
        mock_sleep: AsyncMock,
        mock_subscriber: MagicMock,
        source: MagicMock,
        target: MagicMock,
        metrics: NullMetricsTracker,
    ) -> None:
        tp = TopicPartition("s3mer.replication.sec1", 0)
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["sec1"],
        )

        mock_replicate.side_effect = [
            ConnectionError("transient"),
            None,
        ]

        await _schedule_per_backend_retry(
            subscriber=mock_subscriber,
            failed_tp=tp,
            failed_offset=200,
            message=message,
            operation=S3Operation.PUT_OBJECT,
            source=source,
            target=target,
            metrics=metrics,
        )

        assert mock_sleep.call_count == 2
        mock_subscriber.consumer.seek.assert_called_once_with(tp, 200)
        mock_subscriber.consumer.resume.assert_called_once_with(tp)

    @patch("s3mer.kafka.retry_scheduler.asyncio.sleep", new_callable=AsyncMock)
    @patch("s3mer.kafka.retry_scheduler.replicate_operation", new_callable=AsyncMock)
    async def test_schedule_per_backend_retry_permanent_failure_seeks_forward(
        self,
        mock_replicate: AsyncMock,
        mock_sleep: AsyncMock,
        mock_subscriber: MagicMock,
        source: MagicMock,
        target: MagicMock,
        metrics: NullMetricsTracker,
    ) -> None:
        tp = TopicPartition("s3mer.replication.sec1", 0)
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["sec1"],
        )

        # Fails with AccessDenied
        mock_replicate.side_effect = make_client_error("AccessDenied", 403)

        await _schedule_per_backend_retry(
            subscriber=mock_subscriber,
            failed_tp=tp,
            failed_offset=200,
            message=message,
            operation=S3Operation.PUT_OBJECT,
            source=source,
            target=target,
            metrics=metrics,
        )

        assert mock_sleep.call_count == 1
        # Crucial assert: seeks PAST the failed message offset + 1 to prevent queue lockup!
        mock_subscriber.consumer.seek.assert_called_once_with(tp, 201)
        mock_subscriber.consumer.resume.assert_called_once_with(tp)

    @patch("s3mer.kafka.retry_scheduler.asyncio.sleep", new_callable=AsyncMock)
    @patch("s3mer.kafka.retry_scheduler.replicate_operation", new_callable=AsyncMock)
    async def test_schedule_global_retry_max_retries_advances_offset(
        self,
        mock_replicate: AsyncMock,
        mock_sleep: AsyncMock,
        mock_subscriber: MagicMock,
        source: MagicMock,
        pool: MagicMock,
        metrics: NullMetricsTracker,
    ) -> None:
        ReplicationRetryConfig.max_retries = 2
        tp = TopicPartition("s3mer.replication", 0)
        assigned = {tp}
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["sec1"],
        )
        mock_replicate.side_effect = ConnectionError("transient")

        await _schedule_global_retry(
            subscriber=mock_subscriber,
            failed_tp=tp,
            failed_offset=100,
            assigned_partitions=assigned,
            message=message,
            operation=S3Operation.PUT_OBJECT,
            source=source,
            failed_targets=["sec1"],
            pool=pool,
            metrics=metrics,
        )

        assert mock_sleep.call_count == 2
        mock_subscriber.consumer.seek.assert_called_once_with(tp, 101)
        mock_subscriber.consumer.resume.assert_called_once_with(tp)

    @patch("s3mer.kafka.retry_scheduler.asyncio.sleep", new_callable=AsyncMock)
    @patch("s3mer.kafka.retry_scheduler.replicate_operation", new_callable=AsyncMock)
    async def test_schedule_per_backend_retry_max_retries_advances_offset(
        self,
        mock_replicate: AsyncMock,
        mock_sleep: AsyncMock,
        mock_subscriber: MagicMock,
        source: MagicMock,
        target: MagicMock,
        metrics: NullMetricsTracker,
    ) -> None:
        ReplicationRetryConfig.max_retries = 2
        tp = TopicPartition("s3mer.replication.sec1", 0)
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["sec1"],
        )
        mock_replicate.side_effect = ConnectionError("transient")

        await _schedule_per_backend_retry(
            subscriber=mock_subscriber,
            failed_tp=tp,
            failed_offset=200,
            message=message,
            operation=S3Operation.PUT_OBJECT,
            source=source,
            target=target,
            metrics=metrics,
        )

        assert mock_sleep.call_count == 2
        mock_subscriber.consumer.seek.assert_called_once_with(tp, 201)
        mock_subscriber.consumer.resume.assert_called_once_with(tp)

    async def test_put_object_applies_extended_metadata(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
            metadata={
                "ContentType": "text/plain",
                "ContentEncoding": "gzip",
                "Metadata": {"foo": "bar"},
                "ContentLength": "10",
            },
        )
        body_mock = AsyncMock()
        body_mock.__aenter__.return_value = body_mock
        source.execute.return_value = {"Body": body_mock}

        await _replicate_operation(S3Operation.PUT_OBJECT, message, source, target)

        put_params = target.execute.call_args[0][1]
        assert put_params["ContentEncoding"] == "gzip"
        assert put_params["Metadata"] == {"foo": "bar"}

    async def test_create_bucket_idempotent_replay(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="create_bucket",
            bucket="b",
            key=None,
            source_backend="p",
            target_backends=["s"],
        )
        target.execute.side_effect = make_client_error("BucketAlreadyOwnedByYou", 409)

        await _replicate_operation(S3Operation.CREATE_BUCKET, message, source, target)

        target.execute.assert_called_once()

    async def test_delete_bucket_no_such_bucket_is_idempotent(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="delete_bucket",
            bucket="b",
            key=None,
            source_backend="p",
            target_backends=["s"],
        )
        target.execute.side_effect = make_client_error("NoSuchBucket", 404)

        await _replicate_operation(S3Operation.DELETE_BUCKET, message, source, target)

        target.execute.assert_called_once()

    async def test_unsupported_operation_does_not_raise(self, source: MagicMock, target: MagicMock) -> None:
        message = ReplicationMessage(
            operation="list_objects",
            bucket="b",
            key=None,
            source_backend="p",
            target_backends=["s"],
        )

        await _replicate_operation(S3Operation.LIST_OBJECTS, message, source, target)

        target.execute.assert_not_called()

    async def test_etag_skip_avoids_put(self, source: MagicMock, target: MagicMock) -> None:
        ReplicationRetryConfig.skip_if_etag_matches = True
        message = ReplicationMessage(
            operation="put_object",
            bucket="b",
            key="k",
            source_backend="p",
            target_backends=["s"],
            metadata={},
        )
        source.execute.return_value = {"ETag": '"same"'}
        target.execute.return_value = {"ETag": '"same"'}

        await _replicate_operation(S3Operation.PUT_OBJECT, message, source, target)

        source.execute.assert_called_once()
        target.execute.assert_called_once()
        ReplicationRetryConfig.skip_if_etag_matches = False
