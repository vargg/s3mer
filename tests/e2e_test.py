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


def _test_worker_replication():  # noqa: PLR0915
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

        # Test ListObjectsV2 before deleting bucket
        print("Testing ListObjectsV2...")
        proxy_client.put_object(Bucket=bucket, Key="folder/item1.txt", Body=b"1")
        proxy_client.put_object(Bucket=bucket, Key="folder/item2.txt", Body=b"2")

        list_resp = proxy_client.list_objects_v2(Bucket=bucket, Prefix="folder/")
        assert list_resp["KeyCount"] == 2
        assert list_resp["Contents"][0]["Key"] == "folder/item1.txt"
        print("  ListObjectsV2 OK")

        proxy_client.delete_object(Bucket=bucket, Key="folder/item1.txt")
        proxy_client.delete_object(Bucket=bucket, Key="folder/item2.txt")

        # Test Multipart Upload
        print("Testing Multipart Upload via Proxy...")
        uuid_str = uuid.uuid4().hex
        mp_key = f"multipart-{uuid_str}.txt"
        mp = proxy_client.create_multipart_upload(Bucket=bucket, Key=mp_key)
        upload_id = mp["UploadId"]

        part1_body = b"A" * (5 * 1024 * 1024)  # 5 MiB
        part1 = proxy_client.upload_part(Bucket=bucket, Key=mp_key, PartNumber=1, UploadId=upload_id, Body=part1_body)
        part2 = proxy_client.upload_part(Bucket=bucket, Key=mp_key, PartNumber=2, UploadId=upload_id, Body=b"World!")

        proxy_client.complete_multipart_upload(
            Bucket=bucket,
            Key=mp_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"ETag": part1["ETag"], "PartNumber": 1},
                    {"ETag": part2["ETag"], "PartNumber": 2},
                ]
            },
        )

        # Verify multipart object exists and is complete
        resp = proxy_client.get_object(Bucket=bucket, Key=mp_key)
        expected_body = (b"A" * (5 * 1024 * 1024)) + b"World!"
        assert resp["Body"].read() == expected_body
        print("  Multipart Upload OK")

        # Wait for worker to replicate the fully assembled multipart object
        print("Waiting for worker to replicate assembled multipart object...")
        mp_success = False
        for _ in range(30):
            try:
                resp = secondary_client.head_object(Bucket=bucket, Key=mp_key)
                mp_success = True
                break
            except Exception:
                time.sleep(1)

        if mp_success:
            print("✅ Multipart object successfully replicated to secondary backend!")
        else:
            print("❌ Failed to replicate multipart object")
            sys.exit(1)

        proxy_client.delete_object(Bucket=bucket, Key=mp_key)
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
