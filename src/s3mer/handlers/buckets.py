"""HTTP handlers for S3 bucket operations."""

from urllib.parse import parse_qsl
from xml.etree import ElementTree as ET

from s3mer.common.errors import S3ErrorResponse
from s3mer.common.logging import get_logger
from s3mer.common.responses import ASGIResponse
from s3mer.common.xml import delete_result_xml, list_buckets_xml, list_objects_v2_xml, list_objects_xml
from s3mer.routing.operations import OperationType, S3Operation
from s3mer.routing.registry import BodyStyle, HandlerContext, s3_handler

logger = get_logger(__name__)


@s3_handler(
    S3Operation.CREATE_BUCKET,
    operation_type=OperationType.WRITE,
    is_object_op=False,
)
async def handle_create_bucket(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket} — CreateBucket."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.write_strategy.execute(S3Operation.CREATE_BUCKET, ctx.pool, params)
        return ASGIResponse(
            content=b"",
            status_code=200,
            headers={"Location": f"/{ctx.bucket}"},
        )
    except Exception as exc:
        logger.exception("CreateBucket failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(
    S3Operation.DELETE_BUCKET,
    operation_type=OperationType.WRITE,
    is_object_op=False,
)
async def handle_delete_bucket(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket} — DeleteBucket."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.write_strategy.execute(S3Operation.DELETE_BUCKET, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteBucket failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(
    S3Operation.HEAD_BUCKET,
    operation_type=OperationType.READ,
    is_object_op=False,
)
async def handle_head_bucket(ctx: HandlerContext) -> ASGIResponse:
    """Handle HEAD /{bucket} — HeadBucket."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.read_strategy.execute(S3Operation.HEAD_BUCKET, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("HeadBucket failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(
    S3Operation.LIST_BUCKETS,
    operation_type=OperationType.READ,
    is_object_op=False,
)
async def handle_list_buckets(ctx: HandlerContext) -> ASGIResponse:
    """Handle GET / — ListBuckets."""
    try:
        response = await ctx.read_strategy.execute(S3Operation.LIST_BUCKETS, ctx.pool, {})
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


@s3_handler(
    S3Operation.LIST_OBJECTS_V2,
    operation_type=OperationType.READ,
    is_object_op=False,
)
async def handle_list_objects_v2(ctx: HandlerContext) -> ASGIResponse:
    """Handle GET /{bucket}?list-type=2 — ListObjectsV2."""
    try:
        query = dict(parse_qsl(ctx.query_string.decode("latin-1"), keep_blank_values=True))

        params: dict[str, str] = {"Bucket": ctx.bucket}

        # Pass relevant query params to backend
        if "prefix" in query:
            params["Prefix"] = query["prefix"]
        if "max-keys" in query:
            params["MaxKeys"] = query["max-keys"]  # type: ignore[assignment]
        if "continuation-token" in query:
            params["ContinuationToken"] = query["continuation-token"]

        # We'll just call the backend and pass parameters
        response = await ctx.read_strategy.execute(S3Operation.LIST_OBJECTS_V2, ctx.pool, params)

        xml = list_objects_v2_xml(ctx.bucket, response)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("ListObjectsV2 failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(
    S3Operation.LIST_OBJECTS,
    operation_type=OperationType.READ,
    is_object_op=False,
)
async def handle_list_objects(ctx: HandlerContext) -> ASGIResponse:
    """Handle GET /{bucket} — ListObjects (V1)."""
    try:
        query = dict(parse_qsl(ctx.query_string.decode("latin-1"), keep_blank_values=True))

        params: dict[str, str] = {"Bucket": ctx.bucket}

        if "prefix" in query:
            params["Prefix"] = query["prefix"]
        if "max-keys" in query:
            params["MaxKeys"] = query["max-keys"]  # type: ignore[assignment]
        if "marker" in query:
            params["Marker"] = query["marker"]

        response = await ctx.read_strategy.execute(S3Operation.LIST_OBJECTS, ctx.pool, params)
        xml = list_objects_xml(ctx.bucket, response)
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("ListObjects failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(
    S3Operation.DELETE_OBJECTS,
    operation_type=OperationType.WRITE,
    is_object_op=False,
    body_style=BodyStyle.BUFFERED,
)
async def handle_delete_objects(ctx: HandlerContext) -> ASGIResponse:
    """Handle POST /{bucket}?delete — DeleteObjects."""
    try:
        if not ctx.body:
            return ASGIResponse(
                content=b'<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>', status_code=200
            )

        root = ET.fromstring(ctx.body.decode("utf-8"))

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

        params = {"Bucket": ctx.bucket, "Delete": {"Objects": objects_to_delete}}

        response = await ctx.write_strategy.execute(S3Operation.DELETE_OBJECTS, ctx.pool, params)

        deleted = [d.get("Key", "") for d in response.get("Deleted", [])]
        errors = response.get("Errors", [])
        xml = delete_result_xml(deleted, errors)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("DeleteObjects failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()
