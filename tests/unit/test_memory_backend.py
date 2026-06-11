"""Unit tests for in-memory S3 backend."""

import pytest

from s3mer.backends.memory_backend import MemoryS3BackendClient, clear_memory_store
from s3mer.common.metrics import NullMetricsTracker
from s3mer.routing.operations import S3Operation


@pytest.fixture(autouse=True)
def _clear_store() -> None:
    clear_memory_store()


async def test_put_get_roundtrip() -> None:
    client = MemoryS3BackendClient("mem", is_primary=True, priority=0, metrics=NullMetricsTracker())
    await client.start()
    await client.execute(
        S3Operation.CREATE_BUCKET,
        {"Bucket": "b"},
    )
    await client.execute(
        S3Operation.PUT_OBJECT,
        {"Bucket": "b", "Key": "k", "Body": b"hello", "ContentType": "text/plain"},
    )
    resp = await client.execute(S3Operation.GET_OBJECT, {"Bucket": "b", "Key": "k"})
    assert resp["Body"] == b"hello"
    await client.close()
