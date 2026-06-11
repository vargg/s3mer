"""Application settings loaded from YAML / environment variables."""

import json
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class ReplicationMode(StrEnum):
    """Kafka replication mode strategy."""

    BATCH = "batch"
    PER_BACKEND = "per_backend"


class WriteStrategyType(StrEnum):
    """Write execution strategy style."""

    PRIMARY_REPLICATION = "primary_replication"
    MULTI_SYNC = "multi_sync"  # alias for multi_sync_simple
    MULTI_SYNC_SIMPLE = "multi_sync_simple"
    QUORUM_REPLICATION = "quorum_replication"
    MULTI_SYNC_DISTRIBUTED = "multi_sync_distributed"


class ValkeyConfig(BaseModel):
    """Valkey (Redis-protocol) settings for distributed multipart sessions."""

    url: str = Field(default="valkey://localhost:6379/0")
    session_ttl_seconds: int = Field(default=604800, description="Multipart session TTL (7 days)")
    key_prefix: str = Field(default="s3mer:mpu:")


class BackendType(StrEnum):
    """Backend client implementation."""

    S3 = "s3"
    MEMORY = "memory"


class BackendConfig(BaseModel):
    """Configuration for a single S3-compatible backend (keyed by name in Settings.backends)."""

    backend_type: BackendType = Field(default=BackendType.S3, description="s3 (aiobotocore) or memory (in-process)")
    endpoint_url: str = Field(default="http://localhost:9000", description="S3 endpoint URL")
    region: str = Field(default="us-east-1", description="AWS region")
    access_key: str = Field(description="AWS access key ID")
    secret_key: SecretStr = Field(description="AWS secret access key")
    is_primary: bool = Field(default=False, description="Exactly one backend must be primary")
    addressing_style: str = Field(
        default="path",
        description="S3 addressing style: path or virtual",
    )
    priority: int = Field(
        default=0,
        description="Read priority — lower values are tried first among secondaries",
    )
    max_pool_connections: int = Field(
        default=10,
        description="Max S3 connection pool size",
    )
    connect_timeout: int = Field(
        default=10,
        description="S3 connect timeout in seconds",
    )
    read_timeout: int = Field(
        default=30,
        description="S3 read timeout in seconds",
    )
    max_attempts: int = Field(
        default=2,
        description="Max S3 API retries",
    )
    verify: bool | str = Field(
        default=False,
        description="SSL certificate verification: True/False/path-to-CA-bundle",
    )

    @field_validator("addressing_style")
    @classmethod
    def validate_addressing_style(cls, value: str) -> str:
        if value not in ("path", "virtual"):
            raise ValueError(f"addressing_style must be 'path' or 'virtual', got: {value!r}")
        return value

    @field_validator("verify", mode="before")
    @classmethod
    def parse_verify(cls, value: Any) -> Any:
        if isinstance(value, str):
            val_lower = value.strip().lower()
            if val_lower in ("true", "1", "yes", "on"):
                return True
            if val_lower in ("false", "0", "no", "off"):
                return False
        return value


class CircuitBreakerConfig(BaseModel):
    """Circuit breaker settings for backend failover."""

    enabled: bool = Field(default=True, description="Enable per-backend circuit breakers")
    failure_threshold: int = Field(default=3, ge=1, description="Consecutive failures before opening circuit")
    open_duration_seconds: float = Field(default=30.0, ge=1.0, description="Seconds to skip backend when open")


class KafkaConfig(BaseModel):
    """Kafka connection and topic configuration."""

    bootstrap_servers: list[str] = Field(default=["localhost:9092"])
    topic: str = Field(default="s3mer.replication")
    consumer_group: str = Field(default="s3mer-workers")

    @field_validator("bootstrap_servers", mode="before")
    @classmethod
    def parse_bootstrap_servers(cls, value: Any) -> Any:
        if isinstance(value, str):
            value = value.strip()
            # Try parsing as a JSON array (e.g. '["kafka1:9092"]')
            if value.startswith("[") and value.endswith("]"):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return [str(item).strip() for item in parsed]
                except json.JSONDecodeError:
                    pass
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    concurrency: int = Field(
        default=1,
        ge=1,
        description="Parallel message handlers per consumer (keep at 1 unless ordering is verified)",
    )
    replication_retry_delay: float = Field(
        default=1.0,
        description="Base retry delay in seconds for replication backoff",
    )
    replication_max_retry_delay: float = Field(
        default=60.0,
        description="Max retry delay in seconds for replication backoff",
    )
    replication_max_retries: int = Field(
        default=10,
        ge=1,
        description="Max background retry rounds per message before skipping (uses exponential backoff)",
    )
    replication_skip_if_etag_matches: bool = Field(
        default=False,
        description="Skip PUT replication when live source and target ETags match",
    )
    dlq_enabled: bool = Field(default=True, description="Publish skipped messages to DLQ topics")
    dlq_topic_suffix: str = Field(default=".dlq", description="Suffix appended to replication topic for DLQ")


class WorkerConfig(BaseModel):
    """Configuration for the background replication worker."""

    host: str = Field(default="0.0.0.0", description="Host to bind the worker HTTP server to")  # noqa: S104
    port: int = Field(default=8010, description="Port to bind the worker HTTP server to")


class Settings(BaseSettings):
    """Root application settings."""

    model_config = SettingsConfigDict(
        env_prefix="S3MER_",
        env_nested_delimiter="__",
        # Allow loading from a yaml file if it exists
        yaml_file=Path(__file__).parent.parent.parent.parent.joinpath("config/settings.yaml"),
        extra="ignore",
    )

    backends: dict[str, BackendConfig] = Field(
        default_factory=dict,
        description="S3 backends keyed by logical name (e.g. primary, eu-west)",
    )
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    log_level: str = Field(default="INFO")
    log_file: str | None = Field(
        default=None,
        description="Optional file path to output structured JSON logs",
    )
    replication_mode: ReplicationMode = Field(
        default=ReplicationMode.PER_BACKEND,
        description="Kafka replication strategy: 'batch' (consolidated) or 'per_backend' (individual).",
    )
    write_strategy: WriteStrategyType = Field(
        default=WriteStrategyType.PRIMARY_REPLICATION,
        description=(
            "Write strategy: primary_replication, multi_sync_simple, quorum_replication, or multi_sync_distributed."
        ),
    )
    sync_quorum: int = Field(
        default=1,
        ge=1,
        description="Minimum backends that must acknowledge a synchronous write (quorum / distributed modes).",
    )
    sync_backends: list[str] = Field(
        default_factory=list,
        description="Backends participating in synchronous writes (empty = all backends).",
    )
    response_backend: str | None = Field(
        default=None,
        description="Backend whose response metadata is returned to clients (default: primary among successes).",
    )
    valkey: ValkeyConfig = Field(default_factory=ValkeyConfig)
    stream_chunk_size: int = Field(
        default=65536,
        description="Default streaming chunk size in bytes",
    )
    max_memory_stream_buffer_size: int = Field(
        default=10485760,
        description="Max in-memory stream buffer size in bytes before spooling to disk",
    )
    buffer_dir: str | None = Field(
        default=None,
        description="Base directory for temporary file buffering. Useful for read-only containers.",
    )
    latency_probe_interval_seconds: float = Field(
        default=0.0,
        description="Interval in seconds for active background latency probes.",
    )
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Prioritize environment variables over YAML file."""
        del dotenv_settings
        del file_secret_settings
        return (
            init_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls),
        )

    @field_validator("write_strategy", mode="before")
    @classmethod
    def normalize_write_strategy(cls, value: Any) -> Any:
        if value == "multi_sync":
            return WriteStrategyType.MULTI_SYNC_SIMPLE
        return value

    @model_validator(mode="after")
    def validate_backends(self) -> Self:
        """Ensure exactly one primary backend is configured."""
        if not self.backends:
            return self

        primaries = [name for name, cfg in self.backends.items() if cfg.is_primary]
        if len(primaries) == 0:
            raise ValueError("At least one backend must have is_primary=True")
        if len(primaries) > 1:
            raise ValueError(f"Exactly one primary backend allowed, got: {primaries}")

        sync_backends = self.sync_backends or list(self.backends.keys())
        if self.sync_quorum > len(sync_backends):
            raise ValueError(
                f"sync_quorum ({self.sync_quorum}) cannot exceed number of sync_backends ({len(sync_backends)})"
            )
        for name in sync_backends:
            if name not in self.backends:
                raise ValueError(f"sync_backends references unknown backend: {name!r}")

        if self.response_backend is not None and self.response_backend not in self.backends:
            raise ValueError(f"response_backend references unknown backend: {self.response_backend!r}")

        if self.write_strategy == WriteStrategyType.MULTI_SYNC_DISTRIBUTED and not self.valkey.url:
            raise ValueError("valkey.url is required for multi_sync_distributed")

        if self.replication_mode == ReplicationMode.BATCH and len(self.get_secondaries()) > 1:
            # Warn at validation time via logger in app startup; store flag for startup hook
            pass

        return self

    def get_secondaries(self) -> list[str]:
        """Return names of non-primary backends."""
        return [name for name, cfg in self.backends.items() if not cfg.is_primary]


_settings_override: Settings | None = None


def set_settings_override(settings: Settings | None) -> None:
    """Replace cached settings (for tests). Pass None to restore normal loading."""
    global _settings_override  # noqa: PLW0603
    _settings_override = settings
    load_settings.cache_clear()


@lru_cache
def load_settings() -> Settings:
    """Load settings using Pydantic-settings' built-in resolution logic."""
    if _settings_override is not None:
        return _settings_override
    return Settings()
