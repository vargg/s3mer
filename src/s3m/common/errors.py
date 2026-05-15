"""S3-compatible XML error response builders."""

import uuid
from dataclasses import dataclass, field
from typing import ClassVar, Self

from s3m.common.responses import ASGIResponse


@dataclass(frozen=True, slots=True)
class S3ErrorCode:
    """S3 error code with associated HTTP status and default message."""

    code: str
    http_status: int
    message: str


class S3Errors:
    """Registry of standard S3 error codes."""

    ACCESS_DENIED = S3ErrorCode("AccessDenied", 403, "Access Denied")
    BUCKET_ALREADY_EXISTS = S3ErrorCode("BucketAlreadyExists", 409, "The requested bucket name is not available.")
    BUCKET_ALREADY_OWNED_BY_YOU = S3ErrorCode(
        "BucketAlreadyOwnedByYou",
        409,
        "The bucket you tried to create already exists, and you own it.",
    )
    BUCKET_NOT_EMPTY = S3ErrorCode("BucketNotEmpty", 409, "The bucket you tried to delete is not empty.")
    INTERNAL_ERROR = S3ErrorCode("InternalError", 500, "We encountered an internal error. Please try again.")
    INVALID_BUCKET_NAME = S3ErrorCode("InvalidBucketName", 400, "The specified bucket is not valid.")
    NO_SUCH_BUCKET = S3ErrorCode("NoSuchBucket", 404, "The specified bucket does not exist.")
    NO_SUCH_KEY = S3ErrorCode("NoSuchKey", 404, "The specified key does not exist.")
    METHOD_NOT_ALLOWED = S3ErrorCode("MethodNotAllowed", 405, "The specified method is not allowed.")
    SERVICE_UNAVAILABLE = S3ErrorCode(
        "ServiceUnavailable",
        503,
        "Reduce your request rate. Service is temporarily unavailable.",
    )
    ENTITY_TOO_SMALL = S3ErrorCode(
        "EntityTooSmall",
        400,
        "Your proposed upload is smaller than the minimum allowed object size.",
    )


@dataclass
class S3ErrorResponse:
    """An S3-compatible XML error response."""

    error_code: S3ErrorCode
    message: str | None = None
    resource: str | None = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Map botocore/ClientError error codes to our S3ErrorCode
    BOTOCORE_ERROR_MAP: ClassVar[dict[str, S3ErrorCode]] = {
        "NoSuchBucket": S3Errors.NO_SUCH_BUCKET,
        "NoSuchKey": S3Errors.NO_SUCH_KEY,
        "BucketAlreadyExists": S3Errors.BUCKET_ALREADY_EXISTS,
        "BucketAlreadyOwnedByYou": S3Errors.BUCKET_ALREADY_OWNED_BY_YOU,
        "BucketNotEmpty": S3Errors.BUCKET_NOT_EMPTY,
        "AccessDenied": S3Errors.ACCESS_DENIED,
        "InvalidBucketName": S3Errors.INVALID_BUCKET_NAME,
        "EntityTooSmall": S3Errors.ENTITY_TOO_SMALL,
    }

    def to_xml(self) -> str:
        """Render as S3-compatible XML error body."""
        msg = self.message or self.error_code.message
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<Error>",
            f"  <Code>{self.error_code.code}</Code>",
            f"  <Message>{_xml_escape(msg)}</Message>",
        ]
        if self.resource:
            parts.append(f"  <Resource>{_xml_escape(self.resource)}</Resource>")
        parts.extend(
            [
                f"  <RequestId>{self.request_id}</RequestId>",
                "</Error>",
            ],
        )
        return "\n".join(parts)

    def to_response(self) -> ASGIResponse:
        """Convert to an ASGI response with correct status and content type."""
        return ASGIResponse(
            content=self.to_xml().encode(),
            status_code=self.error_code.http_status,
            media_type="application/xml",
            headers={"x-amz-request-id": self.request_id},
        )

    @classmethod
    def from_client_error(
        cls,
        error: Exception,
        resource: str | None = None,
    ) -> Self:
        """
        Create an S3ErrorResponse from a botocore ClientError.

        Falls back to InternalError for unknown error codes.
        """
        error_code_str = ""
        error_message = str(error)

        # Extract error code from botocore ClientError
        response = getattr(error, "response", None)
        if isinstance(response, dict):
            error_info: dict[str, str] = response.get("Error", {})
            error_code_str = error_info.get("Code", "")
            error_message = error_info.get("Message", str(error))

        s3_error = cls.BOTOCORE_ERROR_MAP.get(error_code_str, S3Errors.INTERNAL_ERROR)

        return cls(
            error_code=s3_error,
            message=error_message,
            resource=resource,
        )


def _xml_escape(text: str) -> str:
    """Escape XML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )
