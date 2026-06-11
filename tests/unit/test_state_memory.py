import pytest

from s3mer.state.memory import MemoryMultipartSessionStore


@pytest.mark.asyncio
async def test_multipart_session_lifecycle() -> None:
    store = MemoryMultipartSessionStore()
    session = await store.create_session("bucket", "key", "canonical-id")
    assert session.canonical_upload_id == "canonical-id"

    await store.set_backend_upload_ids("canonical-id", {"a": "native-a", "b": "native-b"})
    loaded = await store.get_session("canonical-id")
    assert loaded is not None
    assert loaded.backend_upload_ids["a"] == "native-a"

    await store.record_part_etags("canonical-id", 1, {"a": '"etag-a"', "b": '"etag-b"'})
    loaded = await store.get_session("canonical-id")
    assert loaded is not None
    assert loaded.part_etags[1]["b"] == '"etag-b"'

    await store.delete_session("canonical-id")
    assert await store.get_session("canonical-id") is None
