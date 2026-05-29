"""Tests for settings loading and backend configuration shape."""

import json

import pytest
from pydantic import ValidationError

from s3mer.config.settings import Settings


class TestBackendsDictConfig:
    def test_dict_format(self) -> None:
        settings = Settings.model_validate(
            {
                "backends": {
                    "primary": {
                        "endpoint_url": "http://primary:9000",
                        "access_key": "a",
                        "secret_key": "s",
                        "is_primary": True,
                    },
                    "secondary": {
                        "endpoint_url": "http://secondary:9000",
                        "access_key": "a",
                        "secret_key": "s",
                    },
                },
            },
        )
        assert set(settings.backends.keys()) == {"primary", "secondary"}
        assert settings.backends["primary"].is_primary is True

    def test_rejects_list_format(self) -> None:
        with pytest.raises(ValidationError):
            Settings.model_validate(
                {
                    "backends": [
                        {
                            "name": "primary",
                            "endpoint_url": "http://primary:9000",
                            "access_key": "a",
                            "secret_key": "s",
                            "is_primary": True,
                        },
                    ],
                },
            )

    def test_rejects_multiple_primaries(self) -> None:
        with pytest.raises(ValueError, match="Exactly one primary"):
            Settings.model_validate(
                {
                    "backends": {
                        "a": {
                            "endpoint_url": "http://a",
                            "access_key": "a",
                            "secret_key": "s",
                            "is_primary": True,
                        },
                        "b": {
                            "endpoint_url": "http://b",
                            "access_key": "a",
                            "secret_key": "s",
                            "is_primary": True,
                        },
                    },
                },
            )

    def test_env_nested_override_secret_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "S3MER_BACKENDS",
            json.dumps(
                {
                    "primary": {
                        "endpoint_url": "http://primary:9000",
                        "access_key": "file-access",
                        "secret_key": "file-secret",
                        "is_primary": True,
                    },
                    "secondary": {
                        "endpoint_url": "http://secondary:9000",
                        "access_key": "file-access",
                        "secret_key": "file-secret",
                        "is_primary": False,
                    },
                },
            ),
        )
        monkeypatch.setenv("S3MER_BACKENDS__primary__SECRET_KEY", "vault-secret")

        settings = Settings()
        assert settings.backends["primary"].secret_key.get_secret_value() == "vault-secret"
        assert settings.backends["secondary"].secret_key.get_secret_value() == "file-secret"

    def test_verify_parsing(self) -> None:
        # Boolean
        settings = Settings.model_validate(
            {
                "backends": {
                    "primary": {
                        "endpoint_url": "http://primary:9000",
                        "access_key": "a",
                        "secret_key": "s",
                        "is_primary": True,
                        "verify": False,
                    }
                }
            }
        )
        assert settings.backends["primary"].verify is False

        # Boolean string True/False
        settings = Settings.model_validate(
            {
                "backends": {
                    "primary": {
                        "endpoint_url": "http://primary:9000",
                        "access_key": "a",
                        "secret_key": "s",
                        "is_primary": True,
                        "verify": "false",
                    }
                }
            }
        )
        assert settings.backends["primary"].verify is False

        # Path string
        settings = Settings.model_validate(
            {
                "backends": {
                    "primary": {
                        "endpoint_url": "http://primary:9000",
                        "access_key": "a",
                        "secret_key": "s",
                        "is_primary": True,
                        "verify": "/path/to/ca",
                    }
                }
            }
        )
        assert settings.backends["primary"].verify == "/path/to/ca"


class TestKafkaConfig:
    def test_bootstrap_servers_list(self) -> None:
        settings = Settings.model_validate(
            {
                "kafka": {
                    "bootstrap_servers": ["kafka1:9092", "kafka2:9092"],
                }
            }
        )
        assert settings.kafka.bootstrap_servers == ["kafka1:9092", "kafka2:9092"]

    def test_bootstrap_servers_comma_separated(self) -> None:
        settings = Settings.model_validate(
            {
                "kafka": {
                    "bootstrap_servers": "kafka1:9092, kafka2:9092 ,kafka3:9092",
                }
            }
        )
        assert settings.kafka.bootstrap_servers == ["kafka1:9092", "kafka2:9092", "kafka3:9092"]

    def test_bootstrap_servers_json_array(self) -> None:
        settings = Settings.model_validate(
            {
                "kafka": {
                    "bootstrap_servers": '["kafka1:9092", "kafka2:9092"]',
                }
            }
        )
        assert settings.kafka.bootstrap_servers == ["kafka1:9092", "kafka2:9092"]
