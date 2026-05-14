"""S3 XML response builders for successful operations."""

from __future__ import annotations

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
