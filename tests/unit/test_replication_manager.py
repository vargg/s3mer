from unittest.mock import AsyncMock

import pytest

from s3mer.common.metrics import NullMetricsTracker
from s3mer.kafka.manager import BatchReplicationManager, PerBackendReplicationManager
from s3mer.routing.operations import S3Operation


class TestBatchReplicationManager:
    """Tests for the BatchReplicationManager propagation logic."""

    @pytest.fixture
    def publisher(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def manager(self, publisher: AsyncMock) -> BatchReplicationManager:
        return BatchReplicationManager(publisher, NullMetricsTracker())

    async def test_schedule_single_object_replication(
        self,
        manager: BatchReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        params = {"Bucket": "my-bucket", "Key": "hello.txt"}
        response = {"ETag": '"123"'}

        await manager.schedule_replication(
            operation=S3Operation.PUT_OBJECT,
            params=params,
            response=response,
            source_backend_name="primary",
            target_backend_names=["secondary"],
        )

        publisher.publish.assert_called_once()
        msg = publisher.publish.call_args[0][0]
        assert msg.operation == "put_object"
        assert msg.bucket == "my-bucket"
        assert msg.key == "hello.txt"
        assert msg.source_backend == "primary"
        assert msg.target_backends == ["secondary"]
        assert msg.metadata["ETag"] == '"123"'

    async def test_schedule_delete_objects_fan_out(
        self,
        manager: BatchReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        params = {"Bucket": "my-bucket", "Delete": {"Objects": [{"Key": "k1"}, {"Key": "k2"}]}}
        response = {"Deleted": [{"Key": "k1"}, {"Key": "k2"}]}

        await manager.schedule_replication(
            operation=S3Operation.DELETE_OBJECTS,
            params=params,
            response=response,
            source_backend_name="primary",
            target_backend_names=["secondary"],
        )

        assert publisher.publish.call_count == 2  # noqa: PLR2004
        m1 = publisher.publish.call_args_list[0][0][0]
        m2 = publisher.publish.call_args_list[1][0][0]

        assert m1.operation == "delete_object"
        assert m1.key == "k1"
        assert m1.target_backends == ["secondary"]
        assert m2.operation == "delete_object"
        assert m2.key == "k2"
        assert m2.target_backends == ["secondary"]

    async def test_maps_complete_multipart_to_put(
        self,
        manager: BatchReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        params = {"Bucket": "b", "Key": "k"}
        response = {"ETag": '"abc"'}

        await manager.schedule_replication(
            operation=S3Operation.COMPLETE_MULTIPART_UPLOAD,
            params=params,
            response=response,
            source_backend_name="p",
            target_backend_names=["s"],
        )

        msg = publisher.publish.call_args[0][0]
        assert msg.operation == "put_object"

    async def test_does_nothing_if_no_targets(
        self,
        manager: BatchReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        await manager.schedule_replication(
            operation=S3Operation.PUT_OBJECT,
            params={},
            response={},
            source_backend_name="p",
            target_backend_names=[],
        )
        publisher.publish.assert_not_called()


class TestPerBackendReplicationManager:
    """Tests for the PerBackendReplicationManager propagation logic."""

    @pytest.fixture
    def publisher(self) -> AsyncMock:
        pub = AsyncMock()
        pub.topic = "s3mer.replication"
        return pub

    @pytest.fixture
    def manager(self, publisher: AsyncMock) -> PerBackendReplicationManager:
        return PerBackendReplicationManager(publisher, NullMetricsTracker())

    async def test_schedule_single_object_replication(
        self,
        manager: PerBackendReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        params = {"Bucket": "my-bucket", "Key": "hello.txt"}
        response = {"ETag": '"123"'}

        await manager.schedule_replication(
            operation=S3Operation.PUT_OBJECT,
            params=params,
            response=response,
            source_backend_name="primary",
            target_backend_names=["secondary"],
        )

        publisher.publish.assert_called_once()
        msg = publisher.publish.call_args[0][0]
        kwargs = publisher.publish.call_args[1]
        assert msg.operation == "put_object"
        assert msg.bucket == "my-bucket"
        assert msg.key == "hello.txt"
        assert msg.source_backend == "primary"
        assert msg.target_backends == ["secondary"]
        assert msg.metadata["ETag"] == '"123"'
        assert kwargs["topic"] == "s3mer.replication.secondary"

    async def test_schedule_multiple_targets(
        self,
        manager: PerBackendReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        params = {"Bucket": "my-bucket", "Key": "hello.txt"}
        response = {"ETag": '"123"'}

        await manager.schedule_replication(
            operation=S3Operation.PUT_OBJECT,
            params=params,
            response=response,
            source_backend_name="primary",
            target_backend_names=["sec1", "sec2"],
        )

        assert publisher.publish.call_count == 2  # noqa: PLR2004
        m1 = publisher.publish.call_args_list[0][0][0]
        k1 = publisher.publish.call_args_list[0][1]
        m2 = publisher.publish.call_args_list[1][0][0]
        k2 = publisher.publish.call_args_list[1][1]

        assert m1.operation == "put_object"
        assert m1.target_backends == ["sec1"]
        assert k1["topic"] == "s3mer.replication.sec1"
        assert m2.operation == "put_object"
        assert m2.target_backends == ["sec2"]
        assert k2["topic"] == "s3mer.replication.sec2"

    async def test_schedule_delete_objects_fan_out_multiple_targets(
        self,
        manager: PerBackendReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        params = {"Bucket": "my-bucket", "Delete": {"Objects": [{"Key": "k1"}, {"Key": "k2"}]}}
        response = {"Deleted": [{"Key": "k1"}, {"Key": "k2"}]}

        await manager.schedule_replication(
            operation=S3Operation.DELETE_OBJECTS,
            params=params,
            response=response,
            source_backend_name="primary",
            target_backend_names=["sec1", "sec2"],
        )

        # 2 keys * 2 targets = 4 messages published
        assert publisher.publish.call_count == 4  # noqa: PLR2004

        # Message 1: k1 to sec1
        # Message 2: k1 to sec2
        # Message 3: k2 to sec1
        # Message 4: k2 to sec2
        calls = [args[0][0] for args in publisher.publish.call_args_list]
        kwargs = [args[1] for args in publisher.publish.call_args_list]

        assert calls[0].key == "k1"
        assert calls[0].target_backends == ["sec1"]
        assert kwargs[0]["topic"] == "s3mer.replication.sec1"

        assert calls[1].key == "k1"
        assert calls[1].target_backends == ["sec2"]
        assert kwargs[1]["topic"] == "s3mer.replication.sec2"

        assert calls[2].key == "k2"
        assert calls[2].target_backends == ["sec1"]
        assert kwargs[2]["topic"] == "s3mer.replication.sec1"

        assert calls[3].key == "k2"
        assert calls[3].target_backends == ["sec2"]
        assert kwargs[3]["topic"] == "s3mer.replication.sec2"

    async def test_does_nothing_if_no_targets(
        self,
        manager: PerBackendReplicationManager,
        publisher: AsyncMock,
    ) -> None:
        await manager.schedule_replication(
            operation=S3Operation.PUT_OBJECT,
            params={},
            response={},
            source_backend_name="p",
            target_backend_names=[],
        )
        publisher.publish.assert_not_called()
