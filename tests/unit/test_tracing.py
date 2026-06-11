from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka import ConsumerRecord, TopicPartition

from s3mer.app import S3ProxyApp
from s3mer.config.settings import ReplicationMode
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.publisher import ReplicationPublisher
from s3mer.kafka.subscribers import register_subscribers


async def test_proxy_tracing_header_propagation() -> None:
    app = S3ProxyApp()

    # 1. Custom Request ID provided
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/.internal/health",
        "headers": [(b"x-s3mer-request-id", b"custom-req-id-123")],
    }
    receive = AsyncMock()
    send = AsyncMock()

    await app(scope, receive, send)

    # Retrieve response header sent via ASGI start event
    start_call = send.call_args_list[0][0][0]
    assert start_call["type"] == "http.response.start"
    headers = dict(start_call["headers"])
    assert b"x-s3mer-request-id" in headers
    assert headers[b"x-s3mer-request-id"] == b"custom-req-id-123"

    # 2. No Request ID provided - should generate a UUID
    scope_no_id = {
        "type": "http",
        "method": "GET",
        "path": "/.internal/health",
        "headers": [],
    }
    receive_no_id = AsyncMock()
    send_no_id = AsyncMock()

    await app(scope_no_id, receive_no_id, send_no_id)

    start_call_no_id = send_no_id.call_args_list[0][0][0]
    assert start_call_no_id["type"] == "http.response.start"
    headers_no_id = dict(start_call_no_id["headers"])
    assert b"x-s3mer-request-id" in headers_no_id
    # Verify it's a non-empty string/bytes value representing a generated UUID
    generated_id = headers_no_id[b"x-s3mer-request-id"].decode("latin-1")
    assert len(generated_id) > 0


async def test_proxy_tracing_structlog_binding() -> None:
    app = S3ProxyApp()

    with (
        patch("structlog.contextvars.bind_contextvars") as mock_bind,
        patch("structlog.contextvars.clear_contextvars") as mock_clear,
    ):
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/.internal/health",
            "headers": [(b"x-s3mer-request-id", b"my-trace-id-456")],
        }
        receive = AsyncMock()
        send = AsyncMock()

        await app(scope, receive, send)

        mock_bind.assert_called_with(request_id="my-trace-id-456")
        mock_clear.assert_called()


async def test_publisher_tracing_propagation() -> None:
    broker = AsyncMock()
    publisher = ReplicationPublisher(broker=broker, topic="test-topic")

    # Mock structlog contextvars to return our request ID
    with patch("structlog.contextvars.get_contextvars", return_value={"request_id": "kafka-req-id-789"}):
        message = ReplicationMessage(
            operation="put_object",
            bucket="test-bucket",
            key="test-key",
            source_backend="primary",
            target_backends=["secondary"],
        )

        await publisher.publish(message=message)

        # Verify that publish was called with request id in the headers
        broker.publish.assert_called_once()
        kwargs = broker.publish.call_args[1]
        assert "headers" in kwargs
        assert kwargs["headers"] == {"x-s3mer-request-id": "kafka-req-id-789"}


async def test_subscriber_batch_tracing_extraction() -> None:
    # Construct a dummy ConsumerRecord with headers
    record = ConsumerRecord(
        topic="test-topic",
        partition=0,
        offset=42,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=b"{}",
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=0,
        headers=[("x-s3mer-request-id", b"subscriber-req-id-123")],
    )

    msg = MagicMock()
    msg.raw_message = record

    pool = MagicMock()
    subscriber = MagicMock()
    subscriber.consumer = MagicMock()
    subscriber.consumer.assignment.return_value = {TopicPartition("test-topic", 0)}

    mock_broker = MagicMock()
    mock_broker.subscriber.return_value = subscriber

    # Register subscribers to initialize decorators
    register_subscribers(mock_broker, "test-topic", pool, ReplicationMode.BATCH)

    # Get the decorated handler function
    handler = subscriber.call_args_list[0][0][0]

    message = ReplicationMessage(
        operation="put_object",
        bucket="test-bucket",
        key="test-key",
        source_backend="primary",
        target_backends=[],
    )

    with (
        patch("structlog.contextvars.bind_contextvars") as mock_bind,
        patch("structlog.contextvars.clear_contextvars") as mock_clear,
        patch("s3mer.kafka.subscribers.replicate_operation", new_callable=AsyncMock),
    ):
        await handler(message.model_dump_json(), msg)

        # Assert correct request ID was extracted and bound
        mock_bind.assert_called_once_with(request_id="subscriber-req-id-123")
        mock_clear.assert_called_once()
