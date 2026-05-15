#!/usr/bin/env python3
"""
End-to-End Test for the s3m Worker.

Prerequisites:
1. docker-compose up -d (MinIO 1 & 2, Kafka)
2. uv run python -m s3m (Proxy on :8000)
3. uv run python -m s3m.worker.app (Worker)

This script will:
- Connect to the proxy and create a bucket + upload an object.
- Connect to the secondary backend directly (port 9002).
- Poll the secondary backend until the object appears (verifying Kafka replication).
"""

import sys
import time
import urllib.error
import urllib.request
import uuid

import boto3
from botocore.client import Config

PROXY_URL = "http://localhost:8000"
SECONDARY_URL = "http://localhost:9002"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"


def wait_for_replication(s3_client, bucket: str, key: str, timeout: int = 15) -> bool:
    """Poll the S3 client until the object exists or timeout is reached."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            time.sleep(1)
    return False


def _test_metrics_health():
    """Verify metrics and health endpoints."""
    print("Testing /health...")
    req = urllib.request.Request(f"{PROXY_URL}/health")
    with urllib.request.urlopen(req, timeout=5) as response:
        assert response.status == 200
        assert response.read().decode("utf-8") == '{"status":"ok"}'
    print("  /health OK")

    print("Testing /metrics...")
    req = urllib.request.Request(f"{PROXY_URL}/metrics")
    with urllib.request.urlopen(req, timeout=5) as response:
        assert response.status == 200
        assert "s3m_http_requests_total" in response.read().decode("utf-8")
    print("  /metrics OK")


def _test_worker_replication():
    """Verify object is replicated to secondary backend."""
    proxy_client = boto3.client(
        "s3",
        endpoint_url=PROXY_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    secondary_client = boto3.client(
        "s3",
        endpoint_url=SECONDARY_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    bucket = f"e2e-test-{uuid.uuid4().hex[:8]}"
    key = "hello.txt"
    body = b"Replication test!"

    print(f"Creating bucket '{bucket}' via proxy...")
    proxy_client.create_bucket(Bucket=bucket)

    print(f"Uploading object '{key}' via proxy...")
    proxy_client.put_object(Bucket=bucket, Key=key, Body=body)

    print("Waiting for worker to replicate to secondary backend...")
    success = wait_for_replication(secondary_client, bucket, key)

    if success:
        print("✅ Object successfully replicated to secondary backend!")

        # Clean up
        proxy_client.delete_object(Bucket=bucket, Key=key)
        proxy_client.delete_bucket(Bucket=bucket)

        # Give worker a moment to replicate deletion
        time.sleep(2)
    else:
        print("❌ Replication failed or timed out!")
        sys.exit(1)


if __name__ == "__main__":
    try:
        _test_metrics_health()
        _test_worker_replication()
    except Exception as e:
        print(f"Test failed with error: {e}")
        sys.exit(1)
