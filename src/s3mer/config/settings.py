"""Application settings loaded from YAML / environment variables."""

import json
from enum import StrEnum
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
    MULTI_SYNC = "multi_sync"


class BackendConfig(BaseModel):
    """Configuration for a single S3-compatible backend (keyed by name in Settings.backends)."""

    endpoint_url: str = Field(description="S3 endpoint URL")
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

    @field_validator("addressing_style")
    @classmethod
    def validate_addressing_style(cls, value: str) -> str:
        if value not in ("path", "virtual"):
            raise ValueError(f"addressing_style must be 'path' or 'virtual', got: {value!r}")
        return value


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
        description="Number of parallel workers/consumers per process",
    )
    replication_retry_delay: float = Field(
        default=1.0,
        description="Base retry delay in seconds for replication backoff",
    )
    replication_max_retry_delay: float = Field(
        default=60.0,
        description="Max retry delay in seconds for replication backoff",
    )


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
        description="Write strategy mode: 'primary_replication' (default) or 'multi_sync' (concurrent synchronous).",
    )
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
        default=30.0,
        description="Interval in seconds for active background latency probes.",
    )

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

        return self


def load_settings() -> Settings:
    """Load settings using Pydantic-settings' built-in resolution logic."""
    return Settings()
