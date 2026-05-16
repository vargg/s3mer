"""Unit tests for S3 request classifier."""

import pytest

from s3mer.routing.classifier import S3Request, classify_request
from s3mer.routing.operations import S3Operation


class TestClassifyRequest:
    """Test HTTP request → S3 operation classification."""

    # --- Object operations ---

    def test_put_object(self) -> None:
        result = classify_request("PUT", "/my-bucket/photos/cat.jpg")
        assert result == S3Request(
            operation=S3Operation.PUT_OBJECT,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_copy_object(self) -> None:
        result = classify_request(
            "PUT", "/my-bucket/photos/cat.jpg", headers={"x-amz-copy-source": "/other-bucket/cat.jpg"}
        )
        assert result == S3Request(
            operation=S3Operation.COPY_OBJECT,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_get_object(self) -> None:
        result = classify_request("GET", "/my-bucket/photos/cat.jpg")
        assert result == S3Request(
            operation=S3Operation.GET_OBJECT,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_delete_object(self) -> None:
        result = classify_request("DELETE", "/my-bucket/photos/cat.jpg")
        assert result == S3Request(
            operation=S3Operation.DELETE_OBJECT,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_head_object(self) -> None:
        result = classify_request("HEAD", "/my-bucket/photos/cat.jpg")
        assert result == S3Request(
            operation=S3Operation.HEAD_OBJECT,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_get_object_nested_key(self) -> None:
        result = classify_request("GET", "/bucket/a/b/c/d.txt")
        assert result.operation == S3Operation.GET_OBJECT
        assert result.bucket == "bucket"
        assert result.key == "a/b/c/d.txt"

    def test_create_multipart_upload(self) -> None:
        result = classify_request("POST", "/my-bucket/photos/cat.jpg", b"uploads=")
        assert result == S3Request(
            operation=S3Operation.CREATE_MULTIPART_UPLOAD,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_upload_part(self) -> None:
        result = classify_request("PUT", "/my-bucket/photos/cat.jpg", b"partNumber=1&uploadId=123")
        assert result == S3Request(
            operation=S3Operation.UPLOAD_PART,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_complete_multipart_upload(self) -> None:
        result = classify_request("POST", "/my-bucket/photos/cat.jpg", b"uploadId=123")
        assert result == S3Request(
            operation=S3Operation.COMPLETE_MULTIPART_UPLOAD,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_abort_multipart_upload(self) -> None:
        result = classify_request("DELETE", "/my-bucket/photos/cat.jpg", b"uploadId=123")
        assert result == S3Request(
            operation=S3Operation.ABORT_MULTIPART_UPLOAD,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    # --- Tagging operations ---

    def test_put_object_tagging(self) -> None:
        result = classify_request("PUT", "/my-bucket/photos/cat.jpg", b"tagging=")
        assert result == S3Request(
            operation=S3Operation.PUT_OBJECT_TAGGING,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_get_object_tagging(self) -> None:
        result = classify_request("GET", "/my-bucket/photos/cat.jpg", b"tagging=")
        assert result == S3Request(
            operation=S3Operation.GET_OBJECT_TAGGING,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    def test_delete_object_tagging(self) -> None:
        result = classify_request("DELETE", "/my-bucket/photos/cat.jpg", b"tagging=")
        assert result == S3Request(
            operation=S3Operation.DELETE_OBJECT_TAGGING,
            bucket="my-bucket",
            key="photos/cat.jpg",
        )

    # --- Bucket operations ---

    def test_create_bucket(self) -> None:
        result = classify_request("PUT", "/my-bucket")
        assert result == S3Request(
            operation=S3Operation.CREATE_BUCKET,
            bucket="my-bucket",
            key=None,
        )

    def test_list_objects_v2(self) -> None:
        result = classify_request("GET", "/my-bucket", b"list-type=2")
        assert result == S3Request(
            operation=S3Operation.LIST_OBJECTS_V2,
            bucket="my-bucket",
        )

    def test_list_objects_v1(self) -> None:
        result = classify_request("GET", "/my-bucket")
        assert result == S3Request(
            operation=S3Operation.LIST_OBJECTS,
            bucket="my-bucket",
        )

    def test_delete_objects(self) -> None:
        result = classify_request("POST", "/my-bucket", b"delete=")
        assert result == S3Request(
            operation=S3Operation.DELETE_OBJECTS,
            bucket="my-bucket",
        )

    def test_create_bucket_trailing_slash(self) -> None:
        result = classify_request("PUT", "/my-bucket/")
        assert result == S3Request(
            operation=S3Operation.CREATE_BUCKET,
            bucket="my-bucket",
            key=None,
        )

    def test_delete_bucket(self) -> None:
        result = classify_request("DELETE", "/my-bucket")
        assert result == S3Request(
            operation=S3Operation.DELETE_BUCKET,
            bucket="my-bucket",
            key=None,
        )

    def test_head_bucket(self) -> None:
        result = classify_request("HEAD", "/my-bucket")
        assert result == S3Request(
            operation=S3Operation.HEAD_BUCKET,
            bucket="my-bucket",
            key=None,
        )

    # --- Service operations ---

    def test_list_buckets(self) -> None:
        result = classify_request("GET", "/")
        assert result == S3Request(
            operation=S3Operation.LIST_BUCKETS,
            bucket=None,
            key=None,
        )

    # --- Bucket name constraints ---

    def test_rejects_invalid_start_char(self) -> None:
        with pytest.raises(ValueError, match="Cannot classify"):
            classify_request("GET", "/.internal/health")

    def test_rejects_invalid_end_char(self) -> None:
        with pytest.raises(ValueError, match="Cannot classify"):
            classify_request("GET", "/my-bucket.")

    def test_rejects_too_short_name(self) -> None:
        with pytest.raises(ValueError, match="Cannot classify"):
            classify_request("GET", "/ab")  # S3 requires min 3 chars

    def test_rejects_too_long_name(self) -> None:
        long_name = "a" * 64
        with pytest.raises(ValueError, match="Cannot classify"):
            classify_request("GET", f"/{long_name}")

    def test_allows_dots_in_middle(self) -> None:
        result = classify_request("GET", "/my.bucket.name/key")
        assert result.bucket == "my.bucket.name"

    def test_allows_hyphens_in_middle(self) -> None:
        result = classify_request("GET", "/my-bucket-name/key")
        assert result.bucket == "my-bucket-name"
