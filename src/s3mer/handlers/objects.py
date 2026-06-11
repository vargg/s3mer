"""HTTP handlers for S3 object operations."""

from typing import Any
from urllib.parse import parse_qsl
from xml.etree import ElementTree as ET

from s3mer.common.errors import S3ErrorResponse
from s3mer.common.logging import get_logger
from s3mer.common.responses import ASGIResponse, ASGIStreamingResponse
from s3mer.common.streaming import stream_s3_body
from s3mer.common.xml import (
    complete_multipart_upload_xml,
    copy_object_result_xml,
    create_multipart_upload_xml,
    get_object_tagging_xml,
)
from s3mer.routing.operations import S3Operation
from s3mer.routing.registry import BodyStyle, HandlerContext, s3_handler

logger = get_logger(__name__)


@s3_handler(S3Operation.PUT_OBJECT, body_style=BodyStyle.STREAM)
async def handle_put_object(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket}/{key} — PutObject."""
    try:
        content_type = ctx.headers.get("content-type", "application/octet-stream")
        params: dict[str, Any] = {
            "Bucket": ctx.bucket,
            "Key": ctx.key,
            "Body": ctx.body,
            "ContentType": content_type,
        }
        if ctx.content_length is not None:
            params["ContentLength"] = ctx.content_length
        if content_encoding := ctx.headers.get("content-encoding"):
            params["ContentEncoding"] = content_encoding
        if cache_control := ctx.headers.get("cache-control"):
            params["CacheControl"] = cache_control
        if content_disposition := ctx.headers.get("content-disposition"):
            params["ContentDisposition"] = content_disposition
        if content_language := ctx.headers.get("content-language"):
            params["ContentLanguage"] = content_language
        user_metadata = {key[11:]: value for key, value in ctx.headers.items() if key.lower().startswith("x-amz-meta-")}
        if user_metadata:
            params["Metadata"] = user_metadata

        response = await ctx.write_strategy.execute(S3Operation.PUT_OBJECT, ctx.pool, params)

        headers: dict[str, str] = {}
        if "ETag" in response:
            headers["ETag"] = response["ETag"]
        if "VersionId" in response:
            headers["x-amz-version-id"] = response["VersionId"]

        return ASGIResponse(content=b"", status_code=200, headers=headers)
    except Exception as exc:
        logger.exception("PutObject failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.GET_OBJECT)
async def handle_get_object(ctx: HandlerContext) -> ASGIResponse | ASGIStreamingResponse:
    """Handle GET /{bucket}/{key} — GetObject (streaming)."""
    try:
        params = {"Bucket": ctx.bucket, "Key": ctx.key}
        response = await ctx.read_strategy.execute(S3Operation.GET_OBJECT, ctx.pool, params)

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
        logger.exception("GetObject failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.DELETE_OBJECT)
async def handle_delete_object(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket}/{key} — DeleteObject."""
    try:
        params = {"Bucket": ctx.bucket, "Key": ctx.key}
        response = await ctx.write_strategy.execute(S3Operation.DELETE_OBJECT, ctx.pool, params)

        headers: dict[str, str] = {}
        if "VersionId" in response:
            headers["x-amz-version-id"] = response["VersionId"]

        return ASGIResponse(content=b"", status_code=204, headers=headers)
    except Exception as exc:
        logger.exception("DeleteObject failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.HEAD_OBJECT)
async def handle_head_object(ctx: HandlerContext) -> ASGIResponse:
    """Handle HEAD /{bucket}/{key} — HeadObject."""
    try:
        params = {"Bucket": ctx.bucket, "Key": ctx.key}
        response = await ctx.read_strategy.execute(S3Operation.HEAD_OBJECT, ctx.pool, params)

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
        logger.exception("HeadObject failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.CREATE_MULTIPART_UPLOAD)
async def handle_create_multipart_upload(ctx: HandlerContext) -> ASGIResponse:
    """Handle POST /{bucket}/{key}?uploads — CreateMultipartUpload."""
    try:
        params = {"Bucket": ctx.bucket, "Key": ctx.key}
        content_type = ctx.headers.get("content-type")
        if content_type:
            params["ContentType"] = content_type

        response = await ctx.write_strategy.execute(
            S3Operation.CREATE_MULTIPART_UPLOAD, ctx.pool, params, replicate=False
        )

        upload_id = response.get("UploadId", "")
        xml = create_multipart_upload_xml(ctx.bucket, ctx.key or "", upload_id)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("CreateMultipartUpload failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.UPLOAD_PART, body_style=BodyStyle.STREAM)
async def handle_upload_part(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket}/{key}?partNumber=X&uploadId=Y — UploadPart."""
    try:
        query = dict(parse_qsl(ctx.query_string.decode("latin-1"), keep_blank_values=True))

        params: dict[str, Any] = {
            "Bucket": ctx.bucket,
            "Key": ctx.key,
            "Body": ctx.body,
            "PartNumber": int(query.get("partNumber", 0)),
            "UploadId": query.get("uploadId", ""),
        }
        if ctx.content_length is not None:
            params["ContentLength"] = ctx.content_length

        response = await ctx.write_strategy.execute(S3Operation.UPLOAD_PART, ctx.pool, params, replicate=False)

        headers: dict[str, str] = {}
        if "ETag" in response:
            headers["ETag"] = response["ETag"]

        return ASGIResponse(content=b"", status_code=200, headers=headers)
    except Exception as exc:
        logger.exception("UploadPart failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.COMPLETE_MULTIPART_UPLOAD, body_style=BodyStyle.BUFFERED)
async def handle_complete_multipart_upload(ctx: HandlerContext) -> ASGIResponse:
    """Handle POST /{bucket}/{key}?uploadId=Y — CompleteMultipartUpload."""
    try:
        query = dict(parse_qsl(ctx.query_string.decode("latin-1"), keep_blank_values=True))
        upload_id = query.get("uploadId", "")

        # Parse the CompleteMultipartUpload XML payload
        parts = []
        if ctx.body:
            root = ET.fromstring(ctx.body.decode("utf-8"))
            # S3 CompleteMultipartUpload XML structure:
            # <CompleteMultipartUpload xmlns="...">
            #   <Part>
            #     <PartNumber>1</PartNumber>
            #     <ETag>...</ETag>
            #   </Part>
            # </CompleteMultipartUpload>

            # Iterate through all children of the root element
            for part_node in root:
                # Handle potential namespace in tag: {http://...}Part
                if not part_node.tag.endswith("Part"):
                    continue

                part_number = None
                etag = None

                # Look for PartNumber and ETag children regardless of namespace
                for child in part_node:
                    tag = child.tag
                    if tag.endswith("PartNumber") and child.text:
                        try:
                            part_number = int(child.text)
                        except ValueError:
                            logger.warning("Invalid PartNumber in XML", text=child.text)
                    elif tag.endswith("ETag") and child.text:
                        # ETag is often quoted in S3 XML, but we should pass it as provided
                        etag = child.text.strip()

                if part_number is not None and etag is not None:
                    parts.append({"PartNumber": part_number, "ETag": etag})

        params = {"Bucket": ctx.bucket, "Key": ctx.key, "UploadId": upload_id, "MultipartUpload": {"Parts": parts}}

        # replicate=True here because the object is fully assembled!
        response = await ctx.write_strategy.execute(
            S3Operation.COMPLETE_MULTIPART_UPLOAD, ctx.pool, params, replicate=True
        )

        etag = response.get("ETag", "")
        location = response.get("Location", f"/{ctx.bucket}/{ctx.key}")
        xml = complete_multipart_upload_xml(ctx.bucket, ctx.key or "", etag, location)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("CompleteMultipartUpload failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.ABORT_MULTIPART_UPLOAD)
async def handle_abort_multipart_upload(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket}/{key}?uploadId=Y — AbortMultipartUpload."""
    try:
        query = dict(parse_qsl(ctx.query_string.decode("latin-1"), keep_blank_values=True))

        params = {
            "Bucket": ctx.bucket,
            "Key": ctx.key,
            "UploadId": query.get("uploadId", ""),
        }

        await ctx.write_strategy.execute(S3Operation.ABORT_MULTIPART_UPLOAD, ctx.pool, params, replicate=False)
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("AbortMultipartUpload failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.COPY_OBJECT)
async def handle_copy_object(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket}/{key} with x-amz-copy-source — CopyObject."""
    try:
        copy_source = ctx.headers.get("x-amz-copy-source", "")
        source = copy_source.lstrip("/")
        if not source:
            raise ValueError("Empty x-amz-copy-source")  # noqa: TRY301

        params = {"Bucket": ctx.bucket, "Key": ctx.key, "CopySource": source}

        response = await ctx.write_strategy.execute(S3Operation.COPY_OBJECT, ctx.pool, params)

        xml = copy_object_result_xml(response.get("CopyObjectResult", response))
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("CopyObject failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.PUT_OBJECT_TAGGING, body_style=BodyStyle.BUFFERED)
async def handle_put_object_tagging(ctx: HandlerContext) -> ASGIResponse:
    """Handle PUT /{bucket}/{key}?tagging — PutObjectTagging."""
    try:
        if not ctx.body:
            raise ValueError("Empty body in PutObjectTagging")  # noqa: TRY301

        root = ET.fromstring(ctx.body.decode("utf-8"))

        tags = []
        # S3 PutObjectTagging XML structure:
        # <Tagging xmlns="...">
        #   <TagSet>
        #     <Tag>
        #       <Key>...</Key>
        #       <Value>...</Value>
        #     </Tag>
        #   </TagSet>
        # </Tagging>

        # Iterate through children of <Tagging> to find <TagSet>
        for tagging_child in root:
            if not tagging_child.tag.endswith("TagSet"):
                continue

            # Iterate through children of <TagSet> to find <Tag>
            for tag_node in tagging_child:
                if not tag_node.tag.endswith("Tag"):
                    continue

                k = None
                v = None
                # Look for Key and Value regardless of namespace
                for tag_child in tag_node:
                    if tag_child.tag.endswith("Key"):
                        k = tag_child.text
                    elif tag_child.tag.endswith("Value"):
                        v = tag_child.text

                if k is not None and v is not None:
                    tags.append({"Key": k, "Value": v})

        params = {"Bucket": ctx.bucket, "Key": ctx.key, "Tagging": {"TagSet": tags}}

        await ctx.write_strategy.execute(S3Operation.PUT_OBJECT_TAGGING, ctx.pool, params)
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("PutObjectTagging failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.GET_OBJECT_TAGGING)
async def handle_get_object_tagging(ctx: HandlerContext) -> ASGIResponse:
    """Handle GET /{bucket}/{key}?tagging — GetObjectTagging."""
    try:
        response = await ctx.read_strategy.execute(
            S3Operation.GET_OBJECT_TAGGING, ctx.pool, {"Bucket": ctx.bucket, "Key": ctx.key}
        )
        xml = get_object_tagging_xml(response)
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("GetObjectTagging failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")


@s3_handler(S3Operation.DELETE_OBJECT_TAGGING)
async def handle_delete_object_tagging(ctx: HandlerContext) -> ASGIResponse:
    """Handle DELETE /{bucket}/{key}?tagging — DeleteObjectTagging."""
    try:
        await ctx.write_strategy.execute(
            S3Operation.DELETE_OBJECT_TAGGING, ctx.pool, {"Bucket": ctx.bucket, "Key": ctx.key}
        )
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteObjectTagging failed", bucket=ctx.bucket, key=ctx.key, error=str(exc))
        return S3ErrorResponse.from_handler_error(exc, resource=f"/{ctx.bucket}/{ctx.key}")
