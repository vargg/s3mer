
import time
import urllib.request
import uuid
import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.e2e

def test_health_endpoint(s3_config):
    """Verify health endpoint."""
    url = f"{s3_config['endpoint_url']}/health"
    with urllib.request.urlopen(url, timeout=5) as response:
        assert response.status == 200
        assert response.read().decode("utf-8") == '{"status":"ok"}'

def test_metrics_endpoint(s3_config):
    """Verify metrics endpoint."""
    url = f"{s3_config['endpoint_url']}/metrics"
    with urllib.request.urlopen(url, timeout=5) as response:
        assert response.status == 200
        assert "s3m_http_requests_total" in response.read().decode("utf-8")

def test_worker_replication(s3_proxy, s3_secondary):
    """Verify object is replicated to secondary backend."""
    bucket = f"e2e-worker-{uuid.uuid4().hex[:8]}"
    key = "hello.txt"
    body = b"Replication test!"

    # Create bucket and object
    s3_proxy.create_bucket(Bucket=bucket)
    s3_proxy.put_object(Bucket=bucket, Key=key, Body=body)

    # Wait for replication
    success = False
    for _ in range(15):
        try:
            s3_secondary.head_object(Bucket=bucket, Key=key)
            success = True
            break
        except Exception:
            time.sleep(1)

    assert success, "Object was not replicated to secondary"

    # Cleanup
    s3_proxy.delete_object(Bucket=bucket, Key=key)
    s3_proxy.delete_bucket(Bucket=bucket)

def test_multipart_replication(s3_proxy, s3_secondary):
    """Verify fully assembled multipart object is replicated."""
    bucket = f"e2e-mp-repl-{uuid.uuid4().hex[:8]}"
    key = "large.bin"
    s3_proxy.create_bucket(Bucket=bucket)

    # Initiate
    mp = s3_proxy.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mp["UploadId"]

    # Upload parts
    part1_body = b"A" * (5 * 1024 * 1024)
    p1 = s3_proxy.upload_part(Bucket=bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_body)
    p2 = s3_proxy.upload_part(Bucket=bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=b"End")

    # Complete
    s3_proxy.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": p1["ETag"]}, {"PartNumber": 2, "ETag": p2["ETag"]}]}
    )

    # Wait for replication of the full object
    success = False
    for _ in range(30):
        try:
            resp = s3_secondary.get_object(Bucket=bucket, Key=key)
            assert resp["Body"].read() == part1_body + b"End"
            success = True
            break
        except Exception:
            time.sleep(1)

    assert success, "Multipart object was not replicated to secondary"

    # Cleanup
    s3_proxy.delete_object(Bucket=bucket, Key=key)
    s3_proxy.delete_bucket(Bucket=bucket)
