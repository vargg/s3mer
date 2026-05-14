"""HTTP handlers for S3 bucket operations."""

from __future__ import annotations

from s3m.backends.pool import BackendPool
from s3m.common.errors import S3ErrorResponse
from s3m.common.logging import get_logger
from s3m.common.responses import ASGIResponse
from s3m.common.xml import list_buckets_xml
from s3m.routing.operations import S3Operation
from s3m.strategies.read import ReadFallbackStrategy
from s3m.strategies.write import WritePrimaryReplicationStrategy

logger = get_logger(__name__)


async def handle_create_bucket(
    bucket: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
) -> ASGIResponse:
    """Handle PUT /{bucket} — CreateBucket."""
    try:
        params = {"Bucket": bucket}
        await write_strategy.execute(S3Operation.CREATE_BUCKET, pool, params)
        return ASGIResponse(
            content="",
            status_code=200,
            headers={"Location": f"/{bucket}"},
        )
    except Exception as exc:
        logger.error("CreateBucket failed", bucket=bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}").to_response()


async def handle_delete_bucket(
    bucket: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
) -> ASGIResponse:
    """Handle DELETE /{bucket} — DeleteBucket."""
    try:
        params = {"Bucket": bucket}
        await write_strategy.execute(S3Operation.DELETE_BUCKET, pool, params)
        return ASGIResponse(content="", status_code=204)
    except Exception as exc:
        logger.error("DeleteBucket failed", bucket=bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}").to_response()


async def handle_head_bucket(
    bucket: str,
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
) -> ASGIResponse:
    """Handle HEAD /{bucket} — HeadBucket."""
    try:
        params = {"Bucket": bucket}
        await read_strategy.execute(S3Operation.HEAD_BUCKET, pool, params)
        return ASGIResponse(content="", status_code=200)
    except Exception as exc:
        logger.error("HeadBucket failed", bucket=bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}").to_response()


async def handle_list_buckets(
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
) -> ASGIResponse:
    """Handle GET / — ListBuckets."""
    try:
        response = await read_strategy.execute(S3Operation.LIST_BUCKETS, pool, {})
        buckets = response.get("Buckets", [])

        # Convert datetime objects to ISO strings for XML serialization
        bucket_list = []
        for b in buckets:
            bucket_list.append(
                {
                    "Name": b["Name"],
                    "CreationDate": b["CreationDate"].isoformat()
                    if hasattr(b["CreationDate"], "isoformat")
                    else str(b["CreationDate"]),
                }
            )

        xml = list_buckets_xml(bucket_list)
        return ASGIResponse(content=xml, status_code=200)
    except Exception as exc:
        logger.error("ListBuckets failed", error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource="/").to_response()
