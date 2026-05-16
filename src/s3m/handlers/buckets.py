"""HTTP handlers for S3 bucket operations."""

from urllib.parse import parse_qsl
from xml.etree import ElementTree as ET

from s3m.backends.pool import BackendPool
from s3m.common.errors import S3ErrorResponse
from s3m.common.logging import get_logger
from s3m.common.responses import ASGIResponse
from s3m.common.xml import delete_result_xml, list_buckets_xml, list_objects_v2_xml, list_objects_xml
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
            content=b"",
            status_code=200,
            headers={"Location": f"/{bucket}"},
        )
    except Exception as exc:
        logger.exception("CreateBucket failed", bucket=bucket, error=str(exc))
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
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteBucket failed", bucket=bucket, error=str(exc))
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
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("HeadBucket failed", bucket=bucket, error=str(exc))
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
        bucket_list = [
            {
                "Name": b["Name"],
                "CreationDate": b["CreationDate"].isoformat()
                if hasattr(b["CreationDate"], "isoformat")
                else str(b["CreationDate"]),
            }
            for b in buckets
        ]

        xml = list_buckets_xml(bucket_list)
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("ListBuckets failed", error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource="/").to_response()


async def handle_list_objects_v2(
    bucket: str,
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
    query_string: bytes,
) -> ASGIResponse:
    """Handle GET /{bucket}?list-type=2 — ListObjectsV2."""
    try:
        query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))

        params: dict[str, str] = {"Bucket": bucket}

        # Pass relevant query params to backend
        if "prefix" in query:
            params["Prefix"] = query["prefix"]
        if "max-keys" in query:
            params["MaxKeys"] = query["max-keys"]  # type: ignore[assignment]
        if "continuation-token" in query:
            params["ContinuationToken"] = query["continuation-token"]

        # We'll just call the backend and pass parameters
        response = await read_strategy.execute(S3Operation.LIST_OBJECTS_V2, pool, params)

        xml = list_objects_v2_xml(bucket, response)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("ListObjectsV2 failed", bucket=bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}").to_response()


async def handle_list_objects(
    bucket: str,
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
    query_string: bytes,
) -> ASGIResponse:
    """Handle GET /{bucket} — ListObjects (V1)."""
    try:
        query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))

        params: dict[str, str] = {"Bucket": bucket}

        if "prefix" in query:
            params["Prefix"] = query["prefix"]
        if "max-keys" in query:
            params["MaxKeys"] = query["max-keys"]  # type: ignore[assignment]
        if "marker" in query:
            params["Marker"] = query["marker"]

        response = await read_strategy.execute(S3Operation.LIST_OBJECTS, pool, params)
        xml = list_objects_xml(bucket, response)
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("ListObjects failed", bucket=bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}").to_response()


async def handle_delete_objects(
    bucket: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    body: bytes,
) -> ASGIResponse:
    """Handle POST /{bucket}?delete — DeleteObjects."""
    try:
        root = ET.fromstring(body.decode("utf-8"))

        objects_to_delete = []
        # S3 DeleteObjects XML structure:
        # <Delete xmlns="...">
        #   <Object>
        #     <Key>...</Key>
        #   </Object>
        # </Delete>

        # Iterate through all children of the root element (which should be <Delete>)
        for node in root:
            # Handle potential namespace in tag: {http://...}Object
            if not node.tag.endswith("Object"):
                continue

            # Look for Key child regardless of namespace
            for child in node:
                if child.tag.endswith("Key") and child.text:
                    objects_to_delete.append({"Key": child.text})
                    break

        if not objects_to_delete:
            return ASGIResponse(
                content=b'<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>', status_code=200
            )

        params = {"Bucket": bucket, "Delete": {"Objects": objects_to_delete}}

        response = await write_strategy.execute(S3Operation.DELETE_OBJECTS, pool, params)

        deleted = [d.get("Key", "") for d in response.get("Deleted", [])]
        errors = response.get("Errors", [])
        xml = delete_result_xml(deleted, errors)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("DeleteObjects failed", bucket=bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}").to_response()
