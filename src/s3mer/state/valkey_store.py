"""Valkey-backed multipart session store for horizontally scaled multi-sync."""

import json
from typing import Any

from s3mer.config.settings import ValkeyConfig
from s3mer.state.protocol import MultipartSession


class ValkeyMultipartSessionStore:
    """Redis-protocol session store (Valkey-compatible)."""

    def __init__(self, config: ValkeyConfig) -> None:
        self._config = config
        self._client: Any = None

    async def start(self) -> None:
        try:
            from redis.asyncio import from_url  # noqa: PLC0415
        except ImportError as exc:
            msg = (
                "multi_sync_distributed requires the optional 'distributed' extra: uv pip install 's3mer[distributed]'"
            )
            raise RuntimeError(msg) from exc

        self._client = from_url(self._config.url, decode_responses=True)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _key(self, canonical_upload_id: str) -> str:
        return f"{self._config.key_prefix}{canonical_upload_id}"

    def _serialize(self, session: MultipartSession) -> str:
        return json.dumps(
            {
                "bucket": session.bucket,
                "key": session.key,
                "canonical_upload_id": session.canonical_upload_id,
                "backend_upload_ids": session.backend_upload_ids,
                "part_etags": {str(k): v for k, v in session.part_etags.items()},
            }
        )

    def _deserialize(self, payload: str) -> MultipartSession:
        data = json.loads(payload)
        part_etags = {int(part): dict(etags) for part, etags in data.get("part_etags", {}).items()}
        return MultipartSession(
            bucket=data["bucket"],
            key=data["key"],
            canonical_upload_id=data["canonical_upload_id"],
            backend_upload_ids=dict(data.get("backend_upload_ids", {})),
            part_etags=part_etags,
        )

    async def _save(self, session: MultipartSession) -> None:
        if self._client is None:
            raise RuntimeError("ValkeyMultipartSessionStore not started")
        await self._client.set(
            self._key(session.canonical_upload_id),
            self._serialize(session),
            ex=self._config.session_ttl_seconds,
        )

    async def create_session(self, bucket: str, key: str, canonical_upload_id: str) -> MultipartSession:
        session = MultipartSession(bucket=bucket, key=key, canonical_upload_id=canonical_upload_id)
        await self._save(session)
        return session

    async def get_session(self, canonical_upload_id: str) -> MultipartSession | None:
        if self._client is None:
            raise RuntimeError("ValkeyMultipartSessionStore not started")
        payload = await self._client.get(self._key(canonical_upload_id))
        if payload is None:
            return None
        return self._deserialize(payload)

    async def set_backend_upload_ids(self, canonical_upload_id: str, backend_upload_ids: dict[str, str]) -> None:
        session = await self.get_session(canonical_upload_id)
        if session is None:
            raise KeyError(f"Unknown multipart session: {canonical_upload_id}")
        session.backend_upload_ids = dict(backend_upload_ids)
        await self._save(session)

    async def record_part_etags(
        self, canonical_upload_id: str, part_number: int, backend_etags: dict[str, str]
    ) -> None:
        session = await self.get_session(canonical_upload_id)
        if session is None:
            raise KeyError(f"Unknown multipart session: {canonical_upload_id}")
        session.part_etags[part_number] = dict(backend_etags)
        await self._save(session)

    async def delete_session(self, canonical_upload_id: str) -> None:
        if self._client is None:
            raise RuntimeError("ValkeyMultipartSessionStore not started")
        await self._client.delete(self._key(canonical_upload_id))
