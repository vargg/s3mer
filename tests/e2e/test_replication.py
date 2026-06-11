"""E2E replication and failover scenarios."""

import json
import time
import uuid
from collections.abc import Callable

import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError

pytestmark = [pytest.mark.e2e, pytest.mark.replication]


def _wait_for(condition: Callable[[], bool], timeout: float = 30.0, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def test_delete_replication(s3_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-del-{uuid.uuid4().hex[:8]}"
    key = "obj.txt"
    s3_proxy.create_bucket(Bucket=bucket)
    s3_proxy.put_object(Bucket=bucket, Key=key, Body=b"delete-me")

    assert _wait_for(lambda: _head_ok(s3_secondary, bucket, key))

    s3_proxy.delete_object(Bucket=bucket, Key=key)
    assert _wait_for(lambda: not _head_ok(s3_secondary, bucket, key))

    s3_proxy.delete_bucket(Bucket=bucket)


def test_lifecycle_replication(s3_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-lc-{uuid.uuid4().hex[:8]}"
    s3_proxy.create_bucket(Bucket=bucket)
    rule = {
        "Rules": [
            {
                "ID": "expire-1d",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "Expiration": {"Days": 1},
            }
        ]
    }
    s3_proxy.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=rule)

    def lifecycle_on_secondary() -> bool:
        try:
            resp = s3_secondary.get_bucket_lifecycle_configuration(Bucket=bucket)
            return len(resp.get("Rules", [])) == 1
        except ClientError:
            return False

    assert _wait_for(lifecycle_on_secondary)
    s3_proxy.delete_bucket_lifecycle(Bucket=bucket)
    s3_proxy.delete_bucket(Bucket=bucket)


def test_policy_replication(s3_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-pol-{uuid.uuid4().hex[:8]}"
    s3_proxy.create_bucket(Bucket=bucket)
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket}/*",
                }
            ],
        }
    )
    s3_proxy.put_bucket_policy(Bucket=bucket, Policy=policy)

    def policy_on_secondary() -> bool:
        try:
            resp = s3_secondary.get_bucket_policy(Bucket=bucket)
            return bucket in resp.get("Policy", "")
        except ClientError:
            return False

    assert _wait_for(policy_on_secondary)
    s3_proxy.delete_bucket_policy(Bucket=bucket)
    s3_proxy.delete_bucket(Bucket=bucket)


def test_multi_delete_replication(s3_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-md-{uuid.uuid4().hex[:8]}"
    s3_proxy.create_bucket(Bucket=bucket)
    keys = [f"key-{i}.txt" for i in range(3)]
    for key in keys:
        s3_proxy.put_object(Bucket=bucket, Key=key, Body=b"x")

    assert _wait_for(lambda: all(_head_ok(s3_secondary, bucket, k) for k in keys))

    s3_proxy.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": k} for k in keys]},
    )
    assert _wait_for(lambda: not any(_head_ok(s3_secondary, bucket, k) for k in keys))
    s3_proxy.delete_bucket(Bucket=bucket)


def test_copy_object_replication(s3_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-copy-{uuid.uuid4().hex[:8]}"
    src, dst = "source.txt", "dest.txt"
    s3_proxy.create_bucket(Bucket=bucket)
    s3_proxy.put_object(Bucket=bucket, Key=src, Body=b"copy-src")
    s3_proxy.copy_object(Bucket=bucket, Key=dst, CopySource={"Bucket": bucket, "Key": src})

    assert _wait_for(lambda: _head_ok(s3_secondary, bucket, dst))
    s3_proxy.delete_object(Bucket=bucket, Key=src)
    s3_proxy.delete_object(Bucket=bucket, Key=dst)
    s3_proxy.delete_bucket(Bucket=bucket)


def test_metadata_replication(s3_proxy: BaseClient, s3_secondary: BaseClient) -> None:
    bucket = f"e2e-meta-{uuid.uuid4().hex[:8]}"
    key = "meta.bin"
    s3_proxy.create_bucket(Bucket=bucket)
    s3_proxy.put_object(
        Bucket=bucket,
        Key=key,
        Body=b"gzipped",
        Metadata={"custom": "value"},
        ContentEncoding="gzip",
    )

    def metadata_matches() -> bool:
        try:
            resp = s3_secondary.head_object(Bucket=bucket, Key=key)
            return resp.get("Metadata", {}).get("custom") == "value"
        except ClientError:
            return False

    assert _wait_for(metadata_matches)
    s3_proxy.delete_object(Bucket=bucket, Key=key)
    s3_proxy.delete_bucket(Bucket=bucket)


def _head_ok(client: BaseClient, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    else:
        return True
