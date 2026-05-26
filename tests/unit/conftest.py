"""Unit-test fixtures — isolate tests from local config/settings.yaml."""

from collections.abc import Iterator
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from s3mer.config.settings import KafkaConfig, Settings


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
def mock_load_settings(test_settings: Settings) -> Iterator[None]:
    """Prevent S3ProxyApp/worker from loading developer settings.yaml during unit tests."""
    with (
        patch("s3mer.config.settings.load_settings", return_value=test_settings),
        patch("s3mer.app.load_settings", return_value=test_settings),
        patch("s3mer.worker.app.load_settings", return_value=test_settings),
    ):
        yield
