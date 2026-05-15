"""HTTP handlers for S3 object operations."""

import re
from typing import Any
from urllib.parse import parse_qsl
from xml.etree import ElementTree as ET

from s3m.backends.pool import BackendPool
from s3m.common.errors import S3ErrorResponse
from s3m.common.logging import get_logger
from s3m.common.responses import ASGIResponse, ASGIStreamingResponse
from s3m.common.streaming import stream_s3_body
from s3m.common.xml import (
    complete_multipart_upload_xml,
    copy_object_result_xml,
    create_multipart_upload_xml,
    get_object_tagging_xml,
)
from s3m.routing.operations import S3Operation
from s3m.strategies.read import ReadFallbackStrategy
from s3m.strategies.write import WritePrimaryReplicationStrategy

logger = get_logger(__name__)


async def handle_put_object(
    bucket: str,
    key: str,
    body: Any,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    content_type: str = "application/octet-stream",
    content_length: int | None = None,
) -> ASGIResponse:
    """Handle PUT /{bucket}/{key} — PutObject."""
    try:
        params: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
        }
        if content_length is not None:
            params["ContentLength"] = content_length
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


async def handle_create_multipart_upload(
    bucket: str,
    key: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    headers: dict[str, str],
) -> ASGIResponse:
    """Handle POST /{bucket}/{key}?uploads — CreateMultipartUpload."""
    try:
        params = {"Bucket": bucket, "Key": key}
        content_type = headers.get("content-type")
        if content_type:
            params["ContentType"] = content_type

        response = await write_strategy.execute(S3Operation.CREATE_MULTIPART_UPLOAD, pool, params, replicate=False)

        upload_id = response.get("UploadId", "")
        xml = create_multipart_upload_xml(bucket, key, upload_id)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("CreateMultipartUpload failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_upload_part(
    bucket: str,
    key: str,
    body: Any,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    query_string: bytes,
    content_length: int | None = None,
) -> ASGIResponse:
    """Handle PUT /{bucket}/{key}?partNumber=X&uploadId=Y — UploadPart."""
    try:
        query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))

        params: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
            "PartNumber": int(query.get("partNumber", 0)),
            "UploadId": query.get("uploadId", ""),
        }
        if content_length is not None:
            params["ContentLength"] = content_length

        response = await write_strategy.execute(S3Operation.UPLOAD_PART, pool, params, replicate=False)

        headers: dict[str, str] = {}
        if "ETag" in response:
            headers["ETag"] = response["ETag"]

        return ASGIResponse(content=b"", status_code=200, headers=headers)
    except Exception as exc:
        logger.exception("UploadPart failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_complete_multipart_upload(
    bucket: str,
    key: str,
    body: bytes,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    query_string: bytes,
) -> ASGIResponse:
    """Handle POST /{bucket}/{key}?uploadId=Y — CompleteMultipartUpload."""
    try:
        query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))
        upload_id = query.get("uploadId", "")

        # Parse the CompleteMultipartUpload XML payload
        parts = []
        if body:
            root = ET.fromstring(body.decode("utf-8"))
            # xmlns can make tags like {http://s3.amazonaws.com/doc/2006-03-01/}Part
            for part in root.findall(".//Part") or root.findall(".//*[@name='Part']") or root:
                # simple un-namespaced parsing for fallback
                if part.tag.endswith("Part"):
                    p_num = part.find(".//PartNumber")
                    p_num = p_num if p_num is not None else part.find("*[local-name()='PartNumber']")
                    etag = part.find(".//ETag")
                    etag = etag if etag is not None else part.find("*[local-name()='ETag']")

                    # Alternatively, strip namespaces
                    part_str = ET.tostring(part, encoding="unicode")
                    part_num_m = re.search(r"<PartNumber>(\d+)</PartNumber>", part_str)
                    etag_m = re.search(r"<ETag>(.+?)</ETag>", part_str)

                    if part_num_m and etag_m:
                        parts.append({"PartNumber": int(part_num_m.group(1)), "ETag": etag_m.group(1)})

        params = {"Bucket": bucket, "Key": key, "UploadId": upload_id, "MultipartUpload": {"Parts": parts}}

        # replicate=True here because the object is fully assembled!
        response = await write_strategy.execute(S3Operation.COMPLETE_MULTIPART_UPLOAD, pool, params, replicate=True)

        etag = response.get("ETag", "")
        location = response.get("Location", f"/{bucket}/{key}")
        xml = complete_multipart_upload_xml(bucket, key, etag, location)

        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("CompleteMultipartUpload failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_abort_multipart_upload(
    bucket: str,
    key: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    query_string: bytes,
) -> ASGIResponse:
    """Handle DELETE /{bucket}/{key}?uploadId=Y — AbortMultipartUpload."""
    try:
        query = dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))

        params = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": query.get("uploadId", ""),
        }

        await write_strategy.execute(S3Operation.ABORT_MULTIPART_UPLOAD, pool, params, replicate=False)
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("AbortMultipartUpload failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_copy_object(
    bucket: str,
    key: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    copy_source: str,
) -> ASGIResponse:
    """Handle PUT /{bucket}/{key} with x-amz-copy-source — CopyObject."""
    try:
        source = copy_source.lstrip("/")
        if not source:
            msg = "Empty x-amz-copy-source"
            raise ValueError(msg)  # noqa: TRY301

        params = {"Bucket": bucket, "Key": key, "CopySource": source}

        response = await write_strategy.execute(S3Operation.COPY_OBJECT, pool, params)

        xml = copy_object_result_xml(response.get("CopyObjectResult", response))
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("CopyObject failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_put_object_tagging(
    bucket: str,
    key: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
    body: bytes,
) -> ASGIResponse:
    """Handle PUT /{bucket}/{key}?tagging — PutObjectTagging."""
    try:
        root = ET.fromstring(body.decode("utf-8"))

        tags = []
        for tag_elem in root.findall(".//Tag"):
            k = tag_elem.find("Key")
            v = tag_elem.find("Value")
            if k is not None and k.text and v is not None and v.text is not None:
                tags.append({"Key": k.text, "Value": v.text})

        params = {"Bucket": bucket, "Key": key, "Tagging": {"TagSet": tags}}

        await write_strategy.execute(S3Operation.PUT_OBJECT_TAGGING, pool, params)
        return ASGIResponse(content=b"", status_code=200)
    except Exception as exc:
        logger.exception("PutObjectTagging failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_get_object_tagging(
    bucket: str,
    key: str,
    pool: BackendPool,
    read_strategy: ReadFallbackStrategy,
) -> ASGIResponse:
    """Handle GET /{bucket}/{key}?tagging — GetObjectTagging."""
    try:
        response = await read_strategy.execute(S3Operation.GET_OBJECT_TAGGING, pool, {"Bucket": bucket, "Key": key})
        xml = get_object_tagging_xml(response)
        return ASGIResponse(content=xml.encode(), status_code=200)
    except Exception as exc:
        logger.exception("GetObjectTagging failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()


async def handle_delete_object_tagging(
    bucket: str,
    key: str,
    pool: BackendPool,
    write_strategy: WritePrimaryReplicationStrategy,
) -> ASGIResponse:
    """Handle DELETE /{bucket}/{key}?tagging — DeleteObjectTagging."""
    try:
        await write_strategy.execute(S3Operation.DELETE_OBJECT_TAGGING, pool, {"Bucket": bucket, "Key": key})
        return ASGIResponse(content=b"", status_code=204)
    except Exception as exc:
        logger.exception("DeleteObjectTagging failed", bucket=bucket, key=key, error=str(exc))
        return S3ErrorResponse.from_client_error(exc, resource=f"/{bucket}/{key}").to_response()
