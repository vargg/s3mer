"""Application settings loaded from YAML / environment variables."""

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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


class KafkaConfig(BaseModel):
    """Kafka connection and topic configuration."""

    bootstrap_servers: list[str] = Field(default=["localhost:9092"])
    topic: str = Field(default="s3m.replication")
    consumer_group: str = Field(default="s3m-workers")


class Settings(BaseSettings):
    """Root application settings."""

    model_config = SettingsConfigDict(
        env_prefix="S3M_",
        env_nested_delimiter="__",
    )

    backends: list[BackendConfig] = Field(default_factory=list)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    log_level: str = Field(default="INFO")

    @model_validator(mode="after")
    def validate_backends(self) -> Self:
        """Ensure exactly one primary backend is configured."""
        if not self.backends:
            return self

        primaries = [b for b in self.backends if b.is_primary]
        if len(primaries) == 0:
            msg = "At least one backend must have is_primary=True"
            raise ValueError(msg)
        if len(primaries) > 1:
            names = [b.name for b in primaries]
            raise ValueError(f"Exactly one primary backend allowed, got: {names}")

        # Ensure unique names
        names = [b.name for b in self.backends]
        if len(names) != len(set(names)):
            raise ValueError(f"Backend names must be unique, got: {names}")

        return self


def load_settings(config_path: str | Path | None = None) -> Settings:
    """
    Load settings from a YAML file, with environment variable overrides.

    Resolution order:
    1. YAML file (if provided or S3M_CONFIG_PATH env var is set)
    2. Environment variables (override YAML values)
    """
    config_path = config_path or Path(__file__).parent.parent.parent.parent.joinpath("config/settings.yaml")

    if config_path:
        path = Path(config_path)
        if path.exists():
            with path.open() as f:
                data = yaml.safe_load(f) or {}
            return Settings(**data)

    return Settings()
