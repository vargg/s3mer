"""HTTP handlers for S3 bucket operations."""

from urllib.parse import parse_qsl
from xml.etree import ElementTree as ET

from s3mer.common.errors import S3ErrorResponse
from s3mer.common.logging import get_logger
from s3mer.common.responses import ASGIResponse
from s3mer.common.xml import (
    delete_result_xml,
    get_bucket_lifecycle_xml,
    list_buckets_xml,
    list_objects_v2_xml,
    list_objects_xml,
    parse_lifecycle_configuration_xml,
)
from s3mer.routing.operations import S3Operation
from s3mer.routing.registry import BodyStyle, HandlerContext, s3_handler

logger = get_logger(__name__)


@s3_handler(S3Operation.CREATE_BUCKET, is_object_op=False)
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


@s3_handler(S3Operation.DELETE_BUCKET, is_object_op=False)
async def handle_delete_bucket(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket} — DeleteBucket."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.write_strategy.execute(S3Operation.DELETE_BUCKET, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteBucket failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.HEAD_BUCKET, is_object_op=False)
async def handle_head_bucket(ctx: HandlerContext) -> ASGIResponse:
    """Handle HEAD /{bucket} — HeadBucket."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.read_strategy.execute(S3Operation.HEAD_BUCKET, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("HeadBucket failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.LIST_BUCKETS, is_object_op=False)
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


@s3_handler(S3Operation.LIST_OBJECTS_V2, is_object_op=False)
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


@s3_handler(S3Operation.LIST_OBJECTS, is_object_op=False)
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


@s3_handler(S3Operation.DELETE_OBJECTS, is_object_op=False, body_style=BodyStyle.BUFFERED)
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


@s3_handler(S3Operation.GET_BUCKET_LIFECYCLE, is_object_op=False)
async def handle_get_bucket_lifecycle(ctx: HandlerContext) -> ASGIResponse:
    """Handle GET /{bucket}?lifecycle — GetBucketLifecycleConfiguration."""
    try:
        params = {"Bucket": ctx.bucket}
        response = await ctx.read_strategy.execute(S3Operation.GET_BUCKET_LIFECYCLE, ctx.pool, params)
        xml = get_bucket_lifecycle_xml(response)
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("GetBucketLifecycle failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.PUT_BUCKET_LIFECYCLE, is_object_op=False, body_style=BodyStyle.BUFFERED)
async def handle_put_bucket_lifecycle(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket}?lifecycle — PutBucketLifecycleConfiguration."""
    try:
        if not ctx.body:
            raise ValueError("Empty request body for PutBucketLifecycleConfiguration")  # noqa: TRY301

        config = parse_lifecycle_configuration_xml(ctx.body)
        params = {"Bucket": ctx.bucket, "LifecycleConfiguration": config}
        await ctx.write_strategy.execute(S3Operation.PUT_BUCKET_LIFECYCLE, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("PutBucketLifecycle failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.DELETE_BUCKET_LIFECYCLE, is_object_op=False)
async def handle_delete_bucket_lifecycle(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket}?lifecycle — DeleteBucketLifecycle."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.write_strategy.execute(S3Operation.DELETE_BUCKET_LIFECYCLE, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteBucketLifecycle failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.GET_BUCKET_POLICY, is_object_op=False)
async def handle_get_bucket_policy(ctx: HandlerContext) -> ASGIResponse:
    """Handle GET /{bucket}?policy — GetBucketPolicy."""
    try:
        params = {"Bucket": ctx.bucket}
        response = await ctx.read_strategy.execute(S3Operation.GET_BUCKET_POLICY, ctx.pool, params)
        # S3 Policy response returns the policy statement in the 'Policy' field as a JSON string
        policy = response.get("Policy", "")
        # Standard boto3 client returns policy as a string
        content = policy.encode() if isinstance(policy, str) else str(policy).encode()
        return ASGIResponse(content=content, status_code=200, headers={"Content-Type": "application/json"})
    except Exception as exc:
        logger.exception("GetBucketPolicy failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.PUT_BUCKET_POLICY, is_object_op=False, body_style=BodyStyle.BUFFERED)
async def handle_put_bucket_policy(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket}?policy — PutBucketPolicy."""
    try:
        if not ctx.body:
            raise ValueError("Empty request body for PutBucketPolicy")  # noqa: TRY301

        policy_str = ctx.body.decode("utf-8")
        params = {"Bucket": ctx.bucket, "Policy": policy_str}
        await ctx.write_strategy.execute(S3Operation.PUT_BUCKET_POLICY, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("PutBucketPolicy failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()


@s3_handler(S3Operation.DELETE_BUCKET_POLICY, is_object_op=False)
async def handle_delete_bucket_policy(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket}?policy — DeleteBucketPolicy."""
    try:
        params = {"Bucket": ctx.bucket}
        await ctx.write_strategy.execute(S3Operation.DELETE_BUCKET_POLICY, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteBucketPolicy failed", bucket=ctx.bucket, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{ctx.bucket}").to_response()
