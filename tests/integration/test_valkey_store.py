"""Integration tests for Valkey multipart session store."""

import os
import uuid
from collections.abc import AsyncIterator

import pytest

from s3mer.config.settings import ValkeyConfig
from s3mer.state.valkey_store import ValkeyMultipartSessionStore

pytestmark = pytest.mark.integration

VALKEY_URL = os.environ.get("S3MER_VALKEY_URL", "redis://localhost:6379/1")


@pytest.fixture
async def store() -> AsyncIterator[ValkeyMultipartSessionStore]:
    pytest.importorskip("redis")
    config = ValkeyConfig(url=VALKEY_URL, session_ttl_seconds=60, key_prefix="s3mer:test:")
    valkey_store = ValkeyMultipartSessionStore(config)
    try:
        await valkey_store.start()
        yield valkey_store
    except Exception as exc:
        pytest.skip(f"Valkey not available at {VALKEY_URL}: {exc}")
    finally:
        await valkey_store.close()


async def test_session_roundtrip(store: ValkeyMultipartSessionStore) -> None:
    upload_id = str(uuid.uuid4())
    session = await store.create_session("bucket", "key", upload_id)
    assert session.canonical_upload_id == upload_id

    fetched = await store.get_session(upload_id)
    assert fetched is not None
    assert fetched.bucket == "bucket"

    await store.set_backend_upload_ids(upload_id, {"primary": "native-1", "secondary": "native-2"})
    updated = await store.get_session(upload_id)
    assert updated is not None
    assert updated.backend_upload_ids["primary"] == "native-1"

    await store.delete_session(upload_id)
    assert await store.get_session(upload_id) is None
