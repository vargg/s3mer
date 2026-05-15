"""S3 XML response builders for successful operations."""

from datetime import UTC, datetime


def list_buckets_xml(buckets: list[dict]) -> str:
    """
    Build ListBuckets XML response.

    Each bucket dict should have 'Name' and 'CreationDate' keys.
    """
    bucket_entries = []
    for b in buckets:
        name = b["Name"]
        creation_date = b.get("CreationDate", datetime.now(tz=UTC).isoformat())
        bucket_entries.append(f"    <Bucket><Name>{name}</Name><CreationDate>{creation_date}</CreationDate></Bucket>")

    buckets_xml = "\n".join(bucket_entries)

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<ListAllMyBucketsResult>\n"
        "  <Buckets>\n"
        f"{buckets_xml}\n"
        "  </Buckets>\n"
        "</ListAllMyBucketsResult>"
    )


def create_bucket_xml(location: str = "us-east-1") -> str:
    """Build CreateBucket success XML response (Location element)."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<CreateBucketConfiguration>\n"
        f"  <LocationConstraint>{location}</LocationConstraint>\n"
        f"</CreateBucketConfiguration>"
    )


def delete_result_xml(deleted_keys: list[str], errors: list[dict] | None = None) -> str:
    """Build DeleteObjects result XML."""
    parts = ['<?xml version="1.0" encoding="UTF-8"?>', "<DeleteResult>"]

    parts.extend(f"  <Deleted><Key>{key}</Key></Deleted>" for key in deleted_keys)

    if errors:
        for err in errors:
            parts.extend(
                [
                    "  <Error>",
                    f"    <Key>{err['Key']}</Key>",
                    f"    <Code>{err['Code']}</Code>",
                    f"    <Message>{err['Message']}</Message>",
                    "  </Error>",
                ],
            )

    parts.append("</DeleteResult>")
    return "\n".join(parts)


def list_objects_xml(bucket: str, response: dict) -> str:
    """Build ListObjects (V1) XML response."""
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"  <Name>{bucket}</Name>",
    ]

    for key in ["Prefix", "Marker", "MaxKeys", "IsTruncated", "NextMarker"]:
        if key in response:
            val = str(response[key]).lower() if isinstance(response[key], bool) else str(response[key])
            parts.append(f"  <{key}>{val}</{key}>")

    for obj in response.get("Contents", []):
        parts.append("  <Contents>")
        parts.append(f"    <Key>{obj['Key']}</Key>")
        if "LastModified" in obj:
            lm = obj["LastModified"]
            lm_str = lm.isoformat() if hasattr(lm, "isoformat") else str(lm)
            parts.append(f"    <LastModified>{lm_str}</LastModified>")
        if "ETag" in obj:
            parts.append(f"    <ETag>{obj['ETag']}</ETag>")
        if "Size" in obj:
            parts.append(f"    <Size>{obj['Size']}</Size>")
        if "StorageClass" in obj:
            parts.append(f"    <StorageClass>{obj['StorageClass']}</StorageClass>")
        parts.append("  </Contents>")

    parts.append("</ListBucketResult>")
    return "\n".join(parts)


def list_objects_v2_xml(bucket: str, response: dict) -> str:
    """Build ListObjectsV2 XML response."""
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"  <Name>{bucket}</Name>",
    ]

    for key in ["Prefix", "KeyCount", "MaxKeys", "IsTruncated", "ContinuationToken", "NextContinuationToken"]:
        if key in response:
            val = str(response[key]).lower() if isinstance(response[key], bool) else str(response[key])
            parts.append(f"  <{key}>{val}</{key}>")

    for obj in response.get("Contents", []):
        parts.append("  <Contents>")
        parts.append(f"    <Key>{obj['Key']}</Key>")
        if "LastModified" in obj:
            lm = obj["LastModified"]
            lm_str = lm.isoformat() if hasattr(lm, "isoformat") else str(lm)
            parts.append(f"    <LastModified>{lm_str}</LastModified>")
        if "ETag" in obj:
            parts.append(f"    <ETag>{obj['ETag']}</ETag>")
        if "Size" in obj:
            parts.append(f"    <Size>{obj['Size']}</Size>")
        if "StorageClass" in obj:
            parts.append(f"    <StorageClass>{obj['StorageClass']}</StorageClass>")
        parts.append("  </Contents>")

    parts.append("</ListBucketResult>")
    return "\n".join(parts)


def create_multipart_upload_xml(bucket: str, key: str, upload_id: str) -> str:
    """Build InitiateMultipartUploadResult XML."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Bucket>{bucket}</Bucket>\n"
        f"  <Key>{key}</Key>\n"
        f"  <UploadId>{upload_id}</UploadId>\n"
        "</InitiateMultipartUploadResult>"
    )


def complete_multipart_upload_xml(bucket: str, key: str, etag: str, location: str = "") -> str:
    """Build CompleteMultipartUploadResult XML."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Location>{location}</Location>\n"
        f"  <Bucket>{bucket}</Bucket>\n"
        f"  <Key>{key}</Key>\n"
        f"  <ETag>{etag}</ETag>\n"
        "</CompleteMultipartUploadResult>"
    )


def copy_object_result_xml(result: dict) -> str:
    """Build CopyObjectResult XML."""
    etag = result.get("ETag", "")
    last_modified = result.get("LastModified", "")
    lm_str = last_modified.isoformat() if hasattr(last_modified, "isoformat") else str(last_modified)

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<CopyObjectResult>\n"
        f"  <LastModified>{lm_str}</LastModified>\n"
        f"  <ETag>{etag}</ETag>\n"
        "</CopyObjectResult>"
    )
