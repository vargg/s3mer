"""Application settings loaded from YAML / environment variables."""

from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, Field, SecretStr, model_validator
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


class BackendConfig(BaseModel):
    """Configuration for a single S3-compatible backend."""

    name: str = Field(description="Logical name, e.g. 'primary', 'replica-eu'")
    endpoint_url: str = Field(description="S3 endpoint URL")
    region: str = Field(default="us-east-1", description="AWS region")
    access_key: str = Field(description="AWS access key ID")
    secret_key: SecretStr = Field(description="AWS secret access key")
    is_primary: bool = Field(default=False, description="Exactly one backend must be primary")
    addressing_style: Literal["path", "virtual"] = Field(
        default="path",
        description="S3 addressing style",
    )
    priority: int = Field(
        default=0,
        description="Read priority — lower values are tried first",
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


class KafkaConfig(BaseModel):
    """Kafka connection and topic configuration."""

    bootstrap_servers: list[str] = Field(default=["localhost:9092"])
    topic: str = Field(default="s3mer.replication")
    consumer_group: str = Field(default="s3mer-workers")
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

    backends: list[BackendConfig] = Field(default_factory=list)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    log_level: str = Field(default="INFO")
    replication_mode: ReplicationMode = Field(
        default=ReplicationMode.PER_BACKEND,
        description="Kafka replication strategy: 'batch' (consolidated) or 'per_backend' (individual).",
    )
    stream_chunk_size: int = Field(
        default=65536,
        description="Default streaming chunk size in bytes",
    )
    max_memory_stream_buffer_size: int = Field(
        default=10485760,
        description="Max in-memory stream buffer size in bytes before spooling to disk",
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

        primaries = [b for b in self.backends if b.is_primary]
        if len(primaries) == 0:
            raise ValueError("At least one backend must have is_primary=True")
        if len(primaries) > 1:
            names = [b.name for b in primaries]
            raise ValueError(f"Exactly one primary backend allowed, got: {names}")

        # Ensure unique names
        names = [b.name for b in self.backends]
        if len(names) != len(set(names)):
            raise ValueError(f"Backend names must be unique, got: {names}")

        return self


def load_settings() -> Settings:
    """Load settings using Pydantic-settings' built-in resolution logic."""
    return Settings()
