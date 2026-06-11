"""In-memory multipart session store for unit tests and local development."""

import asyncio

from s3mer.state.protocol import MultipartSession


class MemoryMultipartSessionStore:
    """Process-local multipart session store (not for multi-pod production)."""

    def __init__(self) -> None:
        self._sessions: dict[str, MultipartSession] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """No-op for in-memory store."""

    async def close(self) -> None:
        """No-op for in-memory store."""

    async def create_session(self, bucket: str, key: str, canonical_upload_id: str) -> MultipartSession:
        session = MultipartSession(bucket=bucket, key=key, canonical_upload_id=canonical_upload_id)
        async with self._lock:
            self._sessions[canonical_upload_id] = session
        return session

    async def get_session(self, canonical_upload_id: str) -> MultipartSession | None:
        async with self._lock:
            return self._sessions.get(canonical_upload_id)

    async def set_backend_upload_ids(self, canonical_upload_id: str, backend_upload_ids: dict[str, str]) -> None:
        async with self._lock:
            session = self._sessions.get(canonical_upload_id)
            if session is None:
                raise KeyError(f"Unknown multipart session: {canonical_upload_id}")
            session.backend_upload_ids = dict(backend_upload_ids)

    async def record_part_etags(
        self, canonical_upload_id: str, part_number: int, backend_etags: dict[str, str]
    ) -> None:
        async with self._lock:
            session = self._sessions.get(canonical_upload_id)
            if session is None:
                raise KeyError(f"Unknown multipart session: {canonical_upload_id}")
            session.part_etags[part_number] = dict(backend_etags)

    async def delete_session(self, canonical_upload_id: str) -> None:
        async with self._lock:
            self._sessions.pop(canonical_upload_id, None)
