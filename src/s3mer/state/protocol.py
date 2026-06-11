"""Protocols for distributed multipart upload session state."""

from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class MultipartSession:
    """Proxy-issued multipart upload session shared across proxy pods."""

    bucket: str
    key: str
    canonical_upload_id: str
    backend_upload_ids: dict[str, str] = field(default_factory=dict)
    part_etags: dict[int, dict[str, str]] = field(default_factory=dict)


class MultipartSessionStore(Protocol):
    """Storage for canonical upload ID mappings (Valkey or in-memory for tests)."""

    async def start(self) -> None:
        """Open external connections when required."""
        ...

    async def close(self) -> None:
        """Close external connections when required."""
        ...

    async def create_session(self, bucket: str, key: str, canonical_upload_id: str) -> MultipartSession:
        """Persist a new session before backend fan-out completes."""
        ...

    async def get_session(self, canonical_upload_id: str) -> MultipartSession | None:
        """Load a session by proxy-issued upload ID."""
        ...

    async def set_backend_upload_ids(self, canonical_upload_id: str, backend_upload_ids: dict[str, str]) -> None:
        """Store per-backend native upload IDs after CreateMultipartUpload."""
        ...

    async def record_part_etags(
        self, canonical_upload_id: str, part_number: int, backend_etags: dict[str, str]
    ) -> None:
        """Store per-backend ETags returned from UploadPart."""
        ...

    async def delete_session(self, canonical_upload_id: str) -> None:
        """Remove session after complete/abort."""
        ...
