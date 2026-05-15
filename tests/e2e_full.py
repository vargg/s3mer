
import sys
import time
import uuid
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

PROXY_URL = "http://localhost:8000"
SECONDARY_URL = "http://localhost:9002"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

def get_s3_client(url):
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def test_full_s3_api():
    proxy = get_s3_client(PROXY_URL)
    secondary = get_s3_client(SECONDARY_URL)
    
    bucket = f"e2e-full-{uuid.uuid4().hex[:8]}"
    key = "test-object.txt"
    copy_key = "test-object-copy.txt"
    
    print(f"--- Starting Full E2E Test on Bucket: {bucket} ---")
    
    # 1. Bucket Operations
    print("[1] Testing Bucket Operations...")
    proxy.create_bucket(Bucket=bucket)
    print("  CreateBucket OK")
    
    proxy.head_bucket(Bucket=bucket)
    print("  HeadBucket OK")
    
    list_buckets = proxy.list_buckets()
    assert any(b["Name"] == bucket for b in list_buckets["Buckets"])
    print("  ListBuckets OK")
    
    # 2. Object Operations
    print("[2] Testing Object Operations...")
    body = b"Hello S3M!"
    proxy.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/plain")
    print("  PutObject OK")
    
    head_resp = proxy.head_object(Bucket=bucket, Key=key)
    print(f"  HeadObject response: {head_resp}")
    assert head_resp["ContentLength"] == len(body)
    # Some backends might append charset, so we check if it starts with text/plain
    assert head_resp["ContentType"].startswith("text/plain")
    print("  HeadObject OK")
    
    get_resp = proxy.get_object(Bucket=bucket, Key=key)
    assert get_resp["Body"].read() == body
    print("  GetObject OK")
    
    proxy.copy_object(Bucket=bucket, Key=copy_key, CopySource={"Bucket": bucket, "Key": key})
    copy_resp = proxy.get_object(Bucket=bucket, Key=copy_key)
    assert copy_resp["Body"].read() == body
    print("  CopyObject OK")
    
    # 3. Listing Operations
    print("[3] Testing Listing Operations...")
    list_v1 = proxy.list_objects(Bucket=bucket)
    assert len(list_v1["Contents"]) >= 2
    print("  ListObjects (V1) OK")
    
    list_v2 = proxy.list_objects_v2(Bucket=bucket)
    assert list_v2["KeyCount"] >= 2
    print("  ListObjectsV2 OK")
    
    # 4. Tagging Operations
    print("[4] Testing Tagging Operations...")
    tags = {"TagSet": [{"Key": "App", "Value": "S3M"}]}
    proxy.put_object_tagging(Bucket=bucket, Key=key, Tagging=tags)
    print("  PutObjectTagging OK")
    
    get_tags = proxy.get_object_tagging(Bucket=bucket, Key=key)
    assert get_tags["TagSet"][0]["Key"] == "App"
    print("  GetObjectTagging OK")
    
    proxy.delete_object_tagging(Bucket=bucket, Key=key)
    get_tags_after = proxy.get_object_tagging(Bucket=bucket, Key=key)
    assert len(get_tags_after["TagSet"]) == 0
    print("  DeleteObjectTagging OK")
    
    # 5. Multipart Upload
    print("[5] Testing Multipart Upload...")
    mp = proxy.create_multipart_upload(Bucket=bucket, Key="large-file.bin")
    upload_id = mp["UploadId"]
    print("  CreateMultipartUpload OK")
    
    # Use 5MB parts for S3 compliance
    part_size = 5 * 1024 * 1024
    part1 = proxy.upload_part(Bucket=bucket, Key="large-file.bin", PartNumber=1, UploadId=upload_id, Body=b"A" * part_size)
    part2 = proxy.upload_part(Bucket=bucket, Key="large-file.bin", PartNumber=2, UploadId=upload_id, Body=b"B" * 1024)
    print("  UploadPart OK")
    
    proxy.complete_multipart_upload(
        Bucket=bucket,
        Key="large-file.bin",
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part1["ETag"]}, {"PartNumber": 2, "ETag": part2["ETag"]}]}
    )
    print("  CompleteMultipartUpload OK")
    
    # Test AbortMultipartUpload
    mp_abort = proxy.create_multipart_upload(Bucket=bucket, Key="abort-me.bin")
    proxy.abort_multipart_upload(Bucket=bucket, Key="abort-me.bin", UploadId=mp_abort["UploadId"])
    print("  AbortMultipartUpload OK")
    
    # 6. Multi-Delete
    print("[6] Testing Multi-Delete...")
    proxy.delete_objects(
        Bucket=bucket,
        Delete={
            "Objects": [{"Key": key}, {"Key": copy_key}, {"Key": "large-file.bin"}]
        }
    )
    # Verify they are gone
    list_final = proxy.list_objects_v2(Bucket=bucket)
    assert list_final.get("KeyCount", 0) == 0
    print("  DeleteObjects OK")
    
    # 7. Replication Check
    print("[7] Verifying Replication (Eventual Consistency)...")
    # We need to put something and wait
    repl_key = "repl-test.txt"
    proxy.put_object(Bucket=bucket, Key=repl_key, Body=b"Replicated!")
    
    repl_success = False
    for _ in range(30):
        try:
            secondary.head_object(Bucket=bucket, Key=repl_key)
            repl_success = True
            break
        except:
            time.sleep(1)
    
    if repl_success:
        print("  Replication OK")
    else:
        print("  Replication FAILED (Timeout)")
        sys.exit(1)
        
    # Cleanup
    proxy.delete_object(Bucket=bucket, Key=repl_key)
    proxy.delete_bucket(Bucket=bucket)
    print("  Cleanup OK")
    
    print(f"--- All Tests Passed Successfully for {bucket}! ---")

if __name__ == "__main__":
    try:
        test_full_s3_api()
    except Exception as e:
        print(f"\nFATAL: Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
