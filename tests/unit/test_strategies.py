from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError

from s3mer.backends.multisync_strategies import (
    DistributedMultiSyncWriteStrategy,
    QuorumReplicationStrategy,
    SimpleMultiSyncWriteStrategy,
)
from s3mer.backends.strategies import ReadFallbackStrategy, WritePrimaryReplicationStrategy
from s3mer.common.errors import OperationNotSupportedError
from s3mer.common.metrics import NullMetricsTracker
from s3mer.routing.operations import S3Operation
from s3mer.state.memory import MemoryMultipartSessionStore


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
    # Find the designated primary client
    primary_client = next((c for c in clients if c.is_primary), None)
    if primary_client is None and clients:
        primary_client = clients[0]
        primary_client.is_primary = True  # Ensure it is primary in mock client state too!
    pool.primary = primary_client

    pool.get_secondaries.return_value = [c for c in clients if not c.is_primary]
    pool.get_write_candidates.return_value = [pool.primary, *pool.get_secondaries.return_value]
    # For reads, primary is first, then secondaries sorted by priority
    secondaries = sorted(pool.get_secondaries.return_value, key=lambda c: c.priority)
    pool.all_by_latency.return_value = [pool.primary, *secondaries]
    pool.all_clients = clients
    pool.get = lambda name: next(client for client in clients if client.name == name)
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
        c_primary = _make_mock_client("primary", priority=5, is_primary=True)
        c_primary.execute.side_effect = Exception("primary down")

        c_high = _make_mock_client("high-prio", priority=10)
        c_low = _make_mock_client("low-prio", priority=0)
        c_low.execute.return_value = {"Body": b"low-prio-data"}

        pool = _make_mock_pool([c_primary, c_high, c_low])
        result = await strategy.execute(S3Operation.GET_OBJECT, pool, {"Bucket": "b", "Key": "k"})

        assert result == {"Body": b"low-prio-data"}
        c_primary.execute.assert_called_once()
        c_low.execute.assert_called_once()
        c_high.execute.assert_not_called()


class TestWritePrimaryReplicationStrategy:
    """Tests for the write primary + replication strategy."""

    @pytest.fixture
    def replication_manager(self) -> AsyncMock:
        manager = AsyncMock()
        manager.publisher = AsyncMock()
        return manager

    @pytest.fixture
    def strategy(self, replication_manager: AsyncMock) -> WritePrimaryReplicationStrategy:
        return WritePrimaryReplicationStrategy(replication_manager, NullMetricsTracker())

    async def test_writes_to_primary(
        self,
        strategy: WritePrimaryReplicationStrategy,
        replication_manager: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}

        pool = _make_mock_pool([primary])
        pool.get_secondaries.return_value = []

        result = await strategy.execute(S3Operation.PUT_OBJECT, pool, {"Bucket": "b", "Key": "k", "Body": b"data"})

        assert result["ETag"] == '"abc123"'
        primary.execute.assert_called_once()
        replication_manager.schedule_replication.assert_not_called()

    async def test_delegates_replication(
        self,
        strategy: WritePrimaryReplicationStrategy,
        replication_manager: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}
        secondary = _make_mock_client("secondary")

        pool = _make_mock_pool([primary, secondary])

        params = {"Bucket": "b", "Key": "k", "Body": b"data"}
        await strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        replication_manager.schedule_replication.assert_called_once()
        args = replication_manager.schedule_replication.call_args[1]
        assert args["operation"] == S3Operation.PUT_OBJECT
        assert args["source_backend_name"] == "primary"
        assert args["target_backend_names"] == ["secondary"]

    async def test_delegates_replication_on_fallback(
        self,
        strategy: WritePrimaryReplicationStrategy,
        replication_manager: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.side_effect = Exception("Primary down")
        secondary = _make_mock_client("secondary")
        secondary.execute.return_value = {"ETag": '"abc123"'}

        pool = _make_mock_pool([primary, secondary])

        params = {"Bucket": "b", "Key": "k", "Body": b"data"}
        await strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        replication_manager.schedule_replication.assert_called_once()
        args = replication_manager.schedule_replication.call_args[1]
        assert args["source_backend_name"] == "secondary"
        assert "primary" in args["target_backend_names"]

    async def test_does_not_replicate_when_flag_is_false(
        self,
        strategy: WritePrimaryReplicationStrategy,
        replication_manager: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}

        pool = _make_mock_pool([primary])

        await strategy.execute(S3Operation.PUT_OBJECT, pool, {"Bucket": "b", "Key": "k"}, replicate=False)

        replication_manager.schedule_replication.assert_not_called()

    async def test_fails_immediately_on_client_error(
        self,
        strategy: WritePrimaryReplicationStrategy,
        replication_manager: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        exc = ClientError(
            error_response={
                "Error": {"Code": "AccessDenied", "Message": "Access Denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            operation_name="PutObject",
        )
        primary.execute.side_effect = exc

        secondary = _make_mock_client("secondary")
        pool = _make_mock_pool([primary, secondary])

        with pytest.raises(ClientError) as exc_info:
            await strategy.execute(S3Operation.PUT_OBJECT, pool, {"Bucket": "b", "Key": "k"})

        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
        primary.execute.assert_called_once()
        secondary.execute.assert_not_called()
        replication_manager.schedule_replication.assert_not_called()

    async def test_falls_back_on_server_error_and_replicates_to_primary(
        self,
        strategy: WritePrimaryReplicationStrategy,
        replication_manager: AsyncMock,
    ) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        exc = ClientError(
            error_response={
                "Error": {"Code": "InternalError", "Message": "Internal Server Error"},
                "ResponseMetadata": {"HTTPStatusCode": 500},
            },
            operation_name="PutObject",
        )
        primary.execute.side_effect = exc

        secondary = _make_mock_client("secondary")
        secondary.execute.return_value = {"ETag": '"abc123"'}
        pool = _make_mock_pool([primary, secondary])

        params = {"Bucket": "b", "Key": "k", "Body": b"data"}
        result = await strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        assert result["ETag"] == '"abc123"'
        primary.execute.assert_called_once()
        secondary.execute.assert_called_once()

        replication_manager.schedule_replication.assert_called_once()
        args = replication_manager.schedule_replication.call_args[1]
        assert args["source_backend_name"] == "secondary"
        assert args["target_backend_names"] == ["primary"]


class TestSimpleMultiSyncWriteStrategy:
    """Tests for the stateless simple multi-sync write strategy."""

    @pytest.fixture
    def strategy(self) -> SimpleMultiSyncWriteStrategy:
        return SimpleMultiSyncWriteStrategy(NullMetricsTracker(), sync_quorum=1)

    async def test_writes_to_all_backends_concurrently(self, strategy: SimpleMultiSyncWriteStrategy) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}
        secondary = _make_mock_client("secondary")
        secondary.execute.return_value = {"ETag": '"xyz789"'}

        pool = _make_mock_pool([primary, secondary])
        params = {"Bucket": "b", "Key": "k", "Body": b"data"}

        result = await strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        assert result["ETag"] == '"abc123"'
        primary.execute.assert_called_once()
        secondary.execute.assert_called_once()
        assert primary.execute.call_args[0][0] == S3Operation.PUT_OBJECT
        assert isinstance(primary.execute.call_args[0][1]["Body"].__class__.__name__, str)
        assert primary.execute.call_args[0][1]["Body"].__class__.__name__ == "AsyncBytesReader"

    async def test_partial_failure_raises_without_rollback(self, strategy: SimpleMultiSyncWriteStrategy) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"abc123"'}
        secondary = _make_mock_client("secondary")
        secondary.execute.side_effect = Exception("Write to secondary failed")

        pool = _make_mock_pool([primary, secondary])
        params = {"Bucket": "b", "Key": "k", "Body": b"data"}

        with pytest.raises(Exception, match="Write to secondary failed"):
            await strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        primary.execute.assert_called_once()
        assert primary.execute.call_count == 1

    async def test_multipart_is_rejected(self, strategy: SimpleMultiSyncWriteStrategy) -> None:
        pool = _make_mock_pool([_make_mock_client("primary", is_primary=True)])
        with pytest.raises(OperationNotSupportedError):
            await strategy.execute(S3Operation.CREATE_MULTIPART_UPLOAD, pool, {"Bucket": "b", "Key": "k"})

    async def test_streaming_body_buffering(self, strategy: SimpleMultiSyncWriteStrategy) -> None:
        consumed_chunks = []

        async def mock_execute(_op: S3Operation, params: dict[str, Any]) -> dict[str, Any]:
            body = params.get("Body")
            if body:
                nonlocal consumed_chunks
                consumed_chunks = [chunk async for chunk in body]
            return {"ETag": '"abc"'}

        primary = _make_mock_client("primary", is_primary=True)
        primary.execute = AsyncMock(side_effect=mock_execute)
        secondary = _make_mock_client("secondary")
        secondary.execute.return_value = {"ETag": '"xyz"'}

        pool = _make_mock_pool([primary, secondary])

        async def mock_stream() -> AsyncIterator[bytes]:
            yield b"stream"
            yield b"chunks"

        params = {"Bucket": "b", "Key": "k", "Body": mock_stream()}
        await strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        primary.execute.assert_called_once()
        p_body = primary.execute.call_args[0][1]["Body"]
        assert p_body.__class__.__name__ == "AsyncBytesReader"
        assert b"".join(consumed_chunks) == b"streamchunks"


class TestQuorumReplicationStrategy:
    async def test_quorum_success_schedules_async_fill(self) -> None:
        replication_manager = AsyncMock()
        strategy = QuorumReplicationStrategy(
            replication_manager,
            NullMetricsTracker(),
            sync_quorum=1,
        )
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"ETag": '"ok"'}
        secondary = _make_mock_client("secondary")
        secondary.execute.side_effect = Exception("secondary down")

        pool = _make_mock_pool([primary, secondary])
        await strategy.execute(S3Operation.PUT_OBJECT, pool, {"Bucket": "b", "Key": "k", "Body": b"x"})

        replication_manager.schedule_replication.assert_called_once()
        targets = replication_manager.schedule_replication.call_args[1]["target_backend_names"]
        assert targets == ["secondary"]


class TestDistributedMultiSyncWriteStrategy:
    @pytest.fixture
    def strategy(self) -> DistributedMultiSyncWriteStrategy:
        return DistributedMultiSyncWriteStrategy(
            AsyncMock(),
            MemoryMultipartSessionStore(),
            NullMetricsTracker(),
            sync_quorum=2,
        )

    async def test_create_returns_proxy_uuid(self, strategy: DistributedMultiSyncWriteStrategy) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"UploadId": "native-primary"}
        secondary = _make_mock_client("secondary")
        secondary.execute.return_value = {"UploadId": "native-secondary"}

        pool = _make_mock_pool([primary, secondary])
        response = await strategy.execute(
            S3Operation.CREATE_MULTIPART_UPLOAD,
            pool,
            {"Bucket": "b", "Key": "k"},
            replicate=False,
        )

        canonical_id = response["UploadId"]
        assert canonical_id not in {"native-primary", "native-secondary"}
        session = await strategy._session_store.get_session(canonical_id)
        assert session is not None
        assert session.backend_upload_ids == {"primary": "native-primary", "secondary": "native-secondary"}

    async def test_upload_part_maps_backend_upload_ids(self, strategy: DistributedMultiSyncWriteStrategy) -> None:
        primary = _make_mock_client("primary", is_primary=True)
        primary.execute.return_value = {"UploadId": "native-primary"}
        secondary = _make_mock_client("secondary")
        secondary.execute.return_value = {"UploadId": "native-secondary"}
        pool = _make_mock_pool([primary, secondary])

        create_response = await strategy.execute(
            S3Operation.CREATE_MULTIPART_UPLOAD,
            pool,
            {"Bucket": "b", "Key": "k"},
            replicate=False,
        )
        canonical_id = create_response["UploadId"]

        primary.execute.reset_mock()
        secondary.execute.reset_mock()
        primary.execute.return_value = {"ETag": '"etag-p"'}
        secondary.execute.return_value = {"ETag": '"etag-s"'}

        await strategy.execute(
            S3Operation.UPLOAD_PART,
            pool,
            {"Bucket": "b", "Key": "k", "UploadId": canonical_id, "PartNumber": 1, "Body": b"part"},
            replicate=False,
        )

        primary_upload_id = primary.execute.call_args[0][1]["UploadId"]
        secondary_upload_id = secondary.execute.call_args[0][1]["UploadId"]
        assert primary_upload_id != canonical_id
        assert secondary_upload_id != canonical_id
        assert primary_upload_id != secondary_upload_id
