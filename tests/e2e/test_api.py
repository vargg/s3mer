import time
import uuid
from collections.abc import Generator

import pytest
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.exceptions import ClientError

pytestmark = pytest.mark.e2e


@pytest.fixture
def bucket_name() -> str:
    """Generate a unique bucket name for each test."""
    return f"e2e-pytest-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def test_bucket(s3_proxy: BaseClient, bucket_name: str) -> Generator[str, None, None]:
    """Create a bucket and ensure it's deleted after the test."""
    s3_proxy.create_bucket(Bucket=bucket_name)
    yield bucket_name

    # Cleanup: Delete all objects then the bucket
    try:
        response = s3_proxy.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_proxy.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
        s3_proxy.delete_bucket(Bucket=bucket_name)
    except ClientError:
        pass


def test_bucket_lifecycle(s3_proxy: BaseClient, bucket_name: str) -> None:
    """Test CreateBucket, HeadBucket, ListBuckets, DeleteBucket."""
    # Create
    s3_proxy.create_bucket(Bucket=bucket_name)

    # Head
    s3_proxy.head_bucket(Bucket=bucket_name)

    # List
    list_resp = s3_proxy.list_buckets()
    assert any(b["Name"] == bucket_name for b in list_resp["Buckets"])

    # Delete
    s3_proxy.delete_bucket(Bucket=bucket_name)

    # Verify deleted
    with pytest.raises(ClientError) as exc:
        s3_proxy.head_bucket(Bucket=bucket_name)
    assert exc.value.response["Error"]["Code"] in ("404", "NoSuchBucket")


def test_object_operations(s3_proxy: BaseClient, test_bucket: str) -> None:
    """Test PutObject, GetObject, HeadObject, CopyObject, DeleteObject."""
    key = "hello.txt"
    body = b"Hello from Pytest!"

    # Put
    s3_proxy.put_object(Bucket=test_bucket, Key=key, Body=body, ContentType="text/plain")

    # Head
    head = s3_proxy.head_object(Bucket=test_bucket, Key=key)
    assert head["ContentLength"] == len(body)
    assert "text/plain" in head["ContentType"]

    # Get
    get = s3_proxy.get_object(Bucket=test_bucket, Key=key)
    assert get["Body"].read() == body

    # Copy
    copy_key = "hello-copy.txt"
    s3_proxy.copy_object(Bucket=test_bucket, Key=copy_key, CopySource={"Bucket": test_bucket, "Key": key})

    # Verify copy
    get_copy = s3_proxy.get_object(Bucket=test_bucket, Key=copy_key)
    assert get_copy["Body"].read() == body

    # Delete
    s3_proxy.delete_object(Bucket=test_bucket, Key=key)
    s3_proxy.delete_object(Bucket=test_bucket, Key=copy_key)


def test_tagging_operations(s3_proxy: BaseClient, test_bucket: str) -> None:
    """Test PutObjectTagging, GetObjectTagging, DeleteObjectTagging."""
    key = "tag-test.txt"
    s3_proxy.put_object(Bucket=test_bucket, Key=key, Body=b"tags")

    # Put tags
    tags = {"TagSet": [{"Key": "Project", "Value": "S3MER"}]}
    s3_proxy.put_object_tagging(Bucket=test_bucket, Key=key, Tagging=tags)

    # Get tags
    get_tags = s3_proxy.get_object_tagging(Bucket=test_bucket, Key=key)
    assert get_tags["TagSet"][0]["Key"] == "Project"

    # Delete tags
    s3_proxy.delete_object_tagging(Bucket=test_bucket, Key=key)
    get_tags_after = s3_proxy.get_object_tagging(Bucket=test_bucket, Key=key)
    assert len(get_tags_after["TagSet"]) == 0


def test_multipart_upload(s3_proxy: BaseClient, test_bucket: str) -> None:
    """Test multipart upload lifecycle and Abort."""
    key = "large.bin"

    # Create
    mp = s3_proxy.create_multipart_upload(Bucket=test_bucket, Key=key)
    upload_id = mp["UploadId"]

    # Upload parts
    part_size = 5 * 1024 * 1024  # 5MB
    p1 = s3_proxy.upload_part(Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=b"1" * part_size)
    p2 = s3_proxy.upload_part(Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=b"2" * 1024)

    # Complete
    s3_proxy.complete_multipart_upload(
        Bucket=test_bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": p1["ETag"]}, {"PartNumber": 2, "ETag": p2["ETag"]}]},
    )

    # Verify
    head = s3_proxy.head_object(Bucket=test_bucket, Key=key)
    assert head["ContentLength"] == part_size + 1024

    # Test Abort
    abort_key = "abort-me.bin"
    mp_abort = s3_proxy.create_multipart_upload(Bucket=test_bucket, Key=abort_key)
    s3_proxy.abort_multipart_upload(Bucket=test_bucket, Key=abort_key, UploadId=mp_abort["UploadId"])


def test_multi_delete(s3_proxy: BaseClient, s3_resource: ServiceResource, test_bucket: str) -> None:
    """Test DeleteObjects (multi-delete) including resource-style prefix delete."""
    keys = ["multi1.txt", "multi2.txt", "other.txt"]
    for k in keys:
        s3_proxy.put_object(Bucket=test_bucket, Key=k, Body=b"data")

    # Multi-delete two objects
    s3_proxy.delete_objects(Bucket=test_bucket, Delete={"Objects": [{"Key": "multi1.txt"}, {"Key": "multi2.txt"}]})

    # Verify
    list_resp = s3_proxy.list_objects_v2(Bucket=test_bucket)
    remaining = [obj["Key"] for obj in list_resp.get("Contents", [])]
    assert "multi1.txt" not in remaining
    assert "multi2.txt" not in remaining
    assert "other.txt" in remaining

    # Test prefix delete via resource (this exercises the namespace fix)
    s3_proxy.put_object(Bucket=test_bucket, Key="folder/1.txt", Body=b"a")
    s3_proxy.put_object(Bucket=test_bucket, Key="folder/2.txt", Body=b"b")

    bucket_res = s3_resource.Bucket(test_bucket)  # ty: ignore[unresolved-attribute]
    bucket_res.objects.filter(Prefix="folder/").delete()

    list_after = s3_proxy.list_objects_v2(Bucket=test_bucket, Prefix="folder/")
    assert list_after.get("KeyCount", 0) == 0


def test_replication_eventual(s3_proxy: BaseClient, s3_secondary: BaseClient, test_bucket: str) -> None:
    """Verify that objects put through proxy appear on secondary backend."""
    key = "replicated.txt"
    body = b"I will be replicated"

    s3_proxy.put_object(Bucket=test_bucket, Key=key, Body=body)

    # Wait for replication
    success = False
    for _ in range(15):
        try:
            get_repl = s3_secondary.get_object(Bucket=test_bucket, Key=key)
            assert get_repl["Body"].read() == body
            success = True
            break
        except Exception:
            time.sleep(1)

    assert success, "Object was not replicated to secondary within timeout"
