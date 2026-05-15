"""HTTP handlers for S3 object operations."""

from typing import Any

from s3m.backends.pool import BackendPool
from s3m.common.errors import S3ErrorResponse
from s3m.common.logging import get_logger
from s3m.common.responses import ASGIResponse, ASGIStreamingResponse
from s3m.common.streaming import stream_s3_body
from s3m.routing.operations import S3Operation
from s3m.strategies.read import ReadFallbackStrategy
from s3m.strategies.write import WritePrimaryReplicationStrategy

logger = get_logger(__name__)


async def handle_put_object(
    bucket: str,
    key: str,
    body: bytes,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    content_type: str = "application/octet-stream",
) -> ASGIResponse:
    """Handle PUT /{bucket}/{key} — PutObject."""
    try:
        params: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
        }
        response = await write_strategy.execute(S3Operation.PUT_OBJECT, pool, params)

        headers: dict[str, str] = {}
        if "ETag" in response:
            headers["ETag"] = response["ETag"]
        if "VersionId" in response:
            headers["x-amz-version-id"] = response["VersionId"]

        return ASGIResponse(content=b"", status_code=200, headers=headers)
    except Exception as exc:
        logger.exception("PutObject failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_get_object(
    bucket: str,
    key: str,
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
) -> ASGIResponse | ASGIStreamingResponse:
    """Handle GET /{bucket}/{key} — GetObject (streaming)."""
    try:
        params = {"Bucket": bucket, "Key": key}
        response = await read_strategy.execute(S3Operation.GET_OBJECT, pool, params)

        # Build response headers from S3 metadata
        headers: dict[str, str] = {}
        if "ETag" in response:
            headers["ETag"] = response["ETag"]
        if "ContentLength" in response:
            headers["Content-Length"] = str(response["ContentLength"])
        if "LastModified" in response:
            headers["Last-Modified"] = response["LastModified"].strftime("%a, %d %b %Y %H:%M:%S GMT")
        if "VersionId" in response:
            headers["x-amz-version-id"] = response["VersionId"]

        content_type = response.get("ContentType", "application/octet-stream")

        # Stream the body without buffering
        return ASGIStreamingResponse(
            generator=stream_s3_body(response),
            media_type=content_type,
            headers=headers,
            status_code=200,
        )
    except Exception as exc:
        logger.exception("GetObject failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_delete_object(
    bucket: str,
    key: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
) -> ASGIResponse:
    """Handle DELETE /{bucket}/{key} — DeleteObject."""
    try:
        params = {"Bucket": bucket, "Key": key}
        response = await write_strategy.execute(S3Operation.DELETE_OBJECT, pool, params)

        headers: dict[str, str] = {}
        if "VersionId" in response:
            headers["x-amz-version-id"] = response["VersionId"]

        return ASGIResponse(content=b"", status_code=204, headers=headers)
    except Exception as exc:
        logger.exception("DeleteObject failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_head_object(
    bucket: str,
    key: str,
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
) -> ASGIResponse:
    """Handle HEAD /{bucket}/{key} — HeadObject."""
    try:
        params = {"Bucket": bucket, "Key": key}
        response = await read_strategy.execute(S3Operation.HEAD_OBJECT, pool, params)

        headers: dict[str, str] = {}
        if "ETag" in response:
            headers["ETag"] = response["ETag"]
        if "ContentLength" in response:
            headers["Content-Length"] = str(response["ContentLength"])
        if "ContentType" in response:
            headers["Content-Type"] = response["ContentType"]
        if "LastModified" in response:
            headers["Last-Modified"] = response["LastModified"].strftime("%a, %d %b %Y %H:%M:%S GMT")

        return ASGIResponse(content=b"", status_code=200, headers=headers)
    except Exception as exc:
        logger.exception("HeadObject failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()
