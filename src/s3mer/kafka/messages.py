"""Pydantic message schemas for Kafka replication events."""

from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, Field


class ReplicationMessage(BaseModel):
    """
    Message published to Kafka when a write operation succeeds on the primary backend.

    The worker consumes these messages and replicates the operation to target backends.
    For PutObject, the body is NOT included — the worker reads it from the source backend.
    """

    message_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))

    operation: str = Field(description="S3 operation name, e.g. 'put_object'")
    bucket: str = Field(description="Target bucket name")
    key: str | None = Field(default=None, description="Object key (None for bucket ops)")

    source_backend: str = Field(description="Backend that holds the authoritative data")
    target_backends: list[str] = Field(description="Backends to replicate to")

    metadata: dict = Field(default_factory=dict, description="ETag, ContentType, etc.")

    retry_count: int = Field(default=0, description="Number of retry attempts")
