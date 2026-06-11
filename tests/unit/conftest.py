"""Unit-test fixtures — isolate tests from local config/settings.yaml."""

from collections.abc import Iterator

import pytest
from pydantic import SecretStr

from s3mer.common.streaming import reset_stream_config_cache
from s3mer.config.settings import KafkaConfig, Settings, set_settings_override


@pytest.fixture
def test_settings() -> Settings:
    """Minimal settings for unit tests (dict backends, no local YAML required)."""
    return Settings.model_validate(
        {
            "backends": {
                "primary": {
                    "endpoint_url": "http://localhost:9000",
                    "access_key": "test",
                    "secret_key": SecretStr("test"),
                    "is_primary": True,
                },
                "secondary": {
                    "endpoint_url": "http://localhost:9002",
                    "access_key": "test",
                    "secret_key": SecretStr("test"),
                    "is_primary": False,
                },
            },
            "kafka": KafkaConfig(bootstrap_servers=["localhost:9092"]).model_dump(),
        },
    )


@pytest.fixture(autouse=True)
def use_test_settings(test_settings: Settings) -> Iterator[None]:
    """Prevent app/worker code from loading developer settings.yaml during unit tests."""
    set_settings_override(test_settings)
    reset_stream_config_cache()
    yield
    set_settings_override(None)
    reset_stream_config_cache()
