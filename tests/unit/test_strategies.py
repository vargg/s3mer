"""Unit tests for read and write strategies."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from s3m.routing.operations import S3Operation
from s3m.strategies.read import ReadFallbackStrategy
from s3m.strategies.write import WritePrimaryReplicationStrategy


def _make_mock_client(name: str, priority: int = 0, is_primary: bool = False) -> MagicMock:
    """Create a mock S3BackendClient."""
    client = MagicMock()
    client.name = name
    client.priority = priority
    client.is_primary = is_primary
    client.execute = AsyncMock()
    return client


def _make_mock_pool(clients: list[MagicMock]) -> MagicMock:
    """Create a mock BackendPool."""
    pool = MagicMock()
    pool.all_by_priority.return_value = sorted(clients, key=lambda c: c.priority)
    pool.primary = next((c for c in clients if c.is_primary), clients[0])
    pool.get_secondaries.return_value = [c for c in clients if not c.is_primary]
    return pool


class TestReadFallbackStrategy:
    """Tests for the read fallback chain."""

    @pytest.fixture
    def strategy(self) -> ReadFallbackStrategy:
        return ReadFallbackStrategy()

    async def test_returns_first_success(self, strategy: ReadFallbackStrategy) -> None:
        c1 = _make_mock_client("backend-1", priority=0)
        c1.execute.return_value = {"Body": b"data"}
        c2 = _make_mock_client("backend-2", priority=1)

        pool = _make_mock_pool([c1, c2])
        result = await strategy.execute(S3Operation.GET_OBJECT, pool, {"Bucket": "b", "Key": "k"})

        assert result == {"Body": b"data"}
        c1.execute.assert_called_once()
        c2.execute.assert_not_called()

    async def test_falls_back_on_failure(self, strategy: ReadFallbackStrategy) -> None:
        c1 = _make_mock_client("backend-1", priority=0)
        c1.execute.side_effect = Exception("backend-1 down")
        c2 = _make_mock_client("backend-2", priority=1)
        c2.execute.return_value = {"Body": b"data-from-2"}

        pool = _make_mock_pool([c1, c2])
        result = await strategy.execute(S3Operation.GET_OBJECT, pool, {"Bucket": "b", "Key": "k"})

        assert result == {"Body": b"data-from-2"}
        c1.execute.assert_called_once()
        c2.execute.assert_called_once()

    async def test_raises_last_error_when_all_fail(self, strategy: ReadFallbackStrategy) -> None:
        c1 = _make_mock_client("backend-1", priority=0)
        c1.execute.side_effect = Exception("error-1")
        c2 = _make_mock_client("backend-2", priority=1)
        c2.execute.side_effect = Exception("error-2")

        pool = _make_mock_pool([c1, c2])

        with pytest.raises(Exception, match="error-2"):
            await strategy.execute(S3Operation.GET_OBJECT, pool, {"Bucket": "b", "Key": "k"})

    async def test_respects_priority_order(self, strategy: ReadFallbackStrategy) -> None:
        c_high = _make_mock_client("high-prio", priority=10)
        c_low = _make_mock_client("low-prio", priority=0)
        c_low.execute.return_value = {"Body": b"low-prio-data"}

        pool = _make_mock_pool([c_high, c_low])
        result = await strategy.execute(S3Operation.GET_OBJECT, pool, {"Bucket": "b", "Key": "k"})

        assert result == {"Body": b"low-prio-data"}
        c_low.execute.assert_called_once()
        c_high.execute.assert_not_called()


class TestWritePrimaryReplicationStrategy:
    """Tests for the write primary + replication strategy."""

    @pytest.fixture
    def publisher(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def strategy(self, publisher: AsyncMock) -> WritePrimaryReplicationStrategy:
        return WritePrimaryReplicationStrategy(publisher)

    async def test_writes_to_primary(
        self,
        strategy: WritePrimaryReplicationStrategy,
        publisher: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}

        pool = _make_mock_pool([primary])
        pool.get_secondaries.return_value = []

        result = await strategy.execute(
            S3Operation.PUT_OBJECT, pool, {"Bucket": "b", "Key": "k", "Body": b"data"}
        )

        assert result["ETag"] == '"abc123"'
        primary.execute.assert_called_once()
        publisher.publish.assert_not_called()  # no secondaries

    async def test_publishes_replication_message(
        self,
        strategy: WritePrimaryReplicationStrategy,
        publisher: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}
        secondary = _make_mock_client("secondary")

        pool = _make_mock_pool([primary, secondary])

        await strategy.execute(
            S3Operation.PUT_OBJECT, pool, {"Bucket": "b", "Key": "k", "Body": b"data"}
        )

        publisher.publish.assert_called_once()
        msg = publisher.publish.call_args[0][0]
        assert msg.operation == "put_object"
        assert msg.bucket == "b"
        assert msg.key == "k"
        assert msg.source_backend == "primary"
        assert "secondary" in msg.target_backends

    async def test_raises_on_primary_failure(
        self,
        strategy: WritePrimaryReplicationStrategy,
        publisher: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.side_effect = Exception("primary down")

        pool = _make_mock_pool([primary])

        with pytest.raises(Exception, match="primary down"):
            await strategy.execute(
                S3Operation.CREATE_BUCKET, pool, {"Bucket": "b"}
            )

        publisher.publish.assert_not_called()
