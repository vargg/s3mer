from unittest.mock import AsyncMock

import pytest

from s3m.kafka.manager import ReplicationManager
from s3m.routing.operations import S3Operation


class TestReplicationManager:
    """Tests for the ReplicationManager propagation logic."""

    @pytest.fixture
    def publisher(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def manager(self, publisher: AsyncMock) -> ReplicationManager:
        return ReplicationManager(publisher)

    async def test_schedule_single_object_replication(
        self,
        manager: ReplicationManager,
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
        manager: ReplicationManager,
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
        assert m2.operation == "delete_object"
        assert m2.key == "k2"

    async def test_maps_complete_multipart_to_put(
        self,
        manager: ReplicationManager,
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
        manager: ReplicationManager,
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
