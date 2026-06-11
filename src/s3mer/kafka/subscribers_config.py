"""Replication subscriber runtime configuration."""


class ReplicationRetryConfig:
    """Replication retry configuration (set once from Kafka settings at startup)."""

    retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    max_retries: int = 10
    skip_if_etag_matches: bool = False
