"""E2E failover scenarios (require failover proxy with unreachable primary)."""

import os
import time
import uuid

import boto3
import pytest
from botocore.client import BaseClient, Config
from botocore.exceptions import ClientError

pytestmark = [pytest.mark.e2e, pytest.mark.failure]


@pytest.fixture(scope="session")
def s3_failover_proxy(s3_config: dict[str, str]) -> BaseClient | None:
    url = os.environ.get("S3MER_FAILOVER_PROXY_URL")
    if not url:
        pytest.skip("S3MER_FAILOVER_PROXY_URL not set (failover proxy not in compose)")

    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
        config=Config(signature_version="s3v4"),
        region_name=s3_config["region"],
    )


def test_write_fallback(s3_failover_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-wf-{uuid.uuid4().hex[:8]}"
    key = "fallback.txt"
    body = b"written-on-secondary"

    s3_failover_proxy.create_bucket(Bucket=bucket)
    s3_failover_proxy.put_object(Bucket=bucket, Key=key, Body=body)

    resp = s3_secondary.get_object(Bucket=bucket, Key=key)
    assert resp["Body"].read() == body

    s3_failover_proxy.delete_object(Bucket=bucket, Key=key)
    s3_failover_proxy.delete_bucket(Bucket=bucket)


def test_read_fallback(s3_failover_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-rf-{uuid.uuid4().hex[:8]}"
    key = "read-fallback.txt"
    body = b"only-on-secondary"

    s3_secondary.create_bucket(Bucket=bucket)
    s3_secondary.put_object(Bucket=bucket, Key=key, Body=body)

    for _ in range(5):
        try:
            resp = s3_failover_proxy.get_object(Bucket=bucket, Key=key)
            assert resp["Body"].read() == body
            break
        except ClientError:
            time.sleep(1)
    else:
        pytest.fail("Read fallback did not return object from secondary")

    s3_secondary.delete_object(Bucket=bucket, Key=key)
    s3_secondary.delete_bucket(Bucket=bucket)
