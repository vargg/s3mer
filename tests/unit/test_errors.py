# ruff: noqa: PLR2004

from http import HTTPStatus

import aiohttp
from botocore.exceptions import ClientError, ConnectTimeoutError, EndpointConnectionError

from s3mer.common.errors import (
    ErrorAction,
    ErrorClassifier,
    S3ErrorCode,
    S3ErrorResponse,
    S3Errors,
)


def make_client_error(code: str, http_status: int, message: str = "Test S3 error message") -> ClientError:
    """Helper to construct a ClientError with specific code and HTTP status."""
    return ClientError(
        error_response={
            "Error": {
                "Code": code,
                "Message": message,
            },
            "ResponseMetadata": {
                "HTTPStatusCode": http_status,
            },
        },
        operation_name="PutObject",
    )


# ============================================================================
# Part 1: ErrorClassifier Tests
# ============================================================================


def test_classify_client_errors_fail() -> None:
    """Permanent client errors should classify as FAIL."""
    client_errors = [
        ("NoSuchBucket", HTTPStatus.NOT_FOUND),
        ("NoSuchKey", HTTPStatus.NOT_FOUND),
        ("AccessDenied", HTTPStatus.FORBIDDEN),
        ("InvalidBucketName", HTTPStatus.BAD_REQUEST),
        ("EntityTooSmall", HTTPStatus.BAD_REQUEST),
        ("BucketAlreadyExists", HTTPStatus.CONFLICT),
        ("BucketAlreadyOwnedByYou", HTTPStatus.CONFLICT),
        ("BucketNotEmpty", HTTPStatus.CONFLICT),
    ]
    for code, status in client_errors:
        exc = make_client_error(code, status)
        assert ErrorClassifier.classify(exc) == ErrorAction.FAIL


def test_classify_client_errors_retry() -> None:
    """Rate limits and slow-downs should classify as RETRY."""
    transient_errors = [
        ("SlowDown", HTTPStatus.SERVICE_UNAVAILABLE),
        ("RequestLimitExceeded", HTTPStatus.TOO_MANY_REQUESTS),
        ("RequestTimeout", HTTPStatus.REQUEST_TIMEOUT),
        ("Throttling", HTTPStatus.TOO_MANY_REQUESTS),
    ]
    for code, status in transient_errors:
        exc = make_client_error(code, status)
        assert ErrorClassifier.classify(exc) == ErrorAction.RETRY


def test_classify_client_errors_fallback() -> None:
    """Server-side 5xx errors should classify as FALLBACK."""
    server_errors = [
        ("InternalError", HTTPStatus.INTERNAL_SERVER_ERROR),
        ("BadGateway", HTTPStatus.BAD_GATEWAY),
        ("GatewayTimeout", HTTPStatus.GATEWAY_TIMEOUT),
    ]
    for code, status in server_errors:
        exc = make_client_error(code, status)
        assert ErrorClassifier.classify(exc) == ErrorAction.FALLBACK


def test_classify_network_errors_retry() -> None:
    """Network connection, timeout, and client session errors should classify as RETRY."""
    exceptions = [
        TimeoutError(),
        aiohttp.ClientError(),
        EndpointConnectionError(endpoint_url="http://localhost"),
        ConnectTimeoutError(endpoint_url="http://localhost"),
        ConnectionError(),
        OSError(),
    ]
    for exc in exceptions:
        assert ErrorClassifier.classify(exc) == ErrorAction.RETRY


def test_classify_unexpected_exceptions_fallback() -> None:
    """Any unhandled or unexpected Python exception should classify as FALLBACK."""
    exceptions = [
        ValueError("Invalid format"),
        KeyError("key"),
        RuntimeError("System crash"),
    ]
    for exc in exceptions:
        assert ErrorClassifier.classify(exc) == ErrorAction.FALLBACK


def test_classify_client_error_edge_cases() -> None:
    """Test ErrorClassifier edge cases, such as missing response dictionaries."""
    # 1. Missing response attribute completely
    exc_no_response = ClientError({}, "PutObject")
    del exc_no_response.response  # type: ignore[attr-defined]
    assert ErrorClassifier.classify(exc_no_response) == ErrorAction.FAIL

    # 2. Empty response dict (defaults to 0, which classifies as FAIL)
    exc_empty_response = ClientError({}, "PutObject")
    assert ErrorClassifier.classify(exc_empty_response) == ErrorAction.FAIL


# ============================================================================
# Part 2: S3ErrorResponse & XML rendering Tests
# ============================================================================


def test_s3_error_code_dataclass() -> None:
    """Verify that S3ErrorCode slots are correctly formatted."""
    code = S3ErrorCode("TestCode", 400, "Test Message")
    assert code.code == "TestCode"
    assert code.http_status == 400
    assert code.message == "Test Message"


def test_s3_error_response_to_xml() -> None:
    """Verify XML serialization of S3 error response."""
    # 1. Standard error without custom message/resource
    err = S3ErrorResponse(S3Errors.ACCESS_DENIED, request_id="req-123")
    xml_str = err.to_xml()
    assert "<Code>AccessDenied</Code>" in xml_str
    assert f"<Message>{S3Errors.ACCESS_DENIED.message}</Message>" in xml_str
    assert "<RequestId>req-123</RequestId>" in xml_str
    assert "<Resource>" not in xml_str

    # 2. Error with custom message and resource
    err_custom = S3ErrorResponse(
        S3Errors.NO_SUCH_KEY,
        message="Key not found here",
        resource="/mybucket/mykey",
        request_id="req-456",
    )
    xml_custom = err_custom.to_xml()
    assert "<Code>NoSuchKey</Code>" in xml_custom
    assert "<Message>Key not found here</Message>" in xml_custom
    assert "<Resource>/mybucket/mykey</Resource>" in xml_custom
    assert "<RequestId>req-456</RequestId>" in xml_custom


def test_xml_escaping() -> None:
    """Verify that XML characters are correctly escaped to prevent S3 XML injection."""
    err = S3ErrorResponse(
        S3Errors.INVALID_BUCKET_NAME,
        message='Message with <special> & "characters"',
        resource="/bucket-with-'single-quotes'",
    )
    xml_str = err.to_xml()
    # Check escaped characters
    assert "Message with &lt;special&gt; &amp; &quot;characters&quot;" in xml_str
    assert "/bucket-with-&apos;single-quotes&apos;" in xml_str


def test_s3_error_to_asgi_response() -> None:
    """Verify conversion of S3ErrorResponse to ASGI response."""
    err = S3ErrorResponse(S3Errors.SERVICE_UNAVAILABLE, request_id="req-999")
    resp = err.to_response()

    assert resp.status_code == 503
    assert resp.media_type == "application/xml"
    assert resp.extra_headers["x-amz-request-id"] == "req-999"
    body_decoded = resp.body.decode("utf-8")
    assert "<Code>ServiceUnavailable</Code>" in body_decoded
    assert "<RequestId>req-999</RequestId>" in body_decoded


def test_from_client_error_mapping() -> None:
    """Verify from_client_error handles exact mapping and fallback to InternalError."""
    # 1. Exact mapping of S3 code
    exc_access_denied = make_client_error("AccessDenied", 403, "Access is denied.")
    err_resp = S3ErrorResponse.from_client_error(exc_access_denied, resource="/mybucket")
    assert err_resp.error_code == S3Errors.ACCESS_DENIED
    assert err_resp.message == "Access is denied."
    assert err_resp.resource == "/mybucket"

    # 2. Unknown code fallbacks to InternalError
    exc_unknown = make_client_error("UnknownErrorCode", 418, "Tea pot.")
    err_unknown = S3ErrorResponse.from_client_error(exc_unknown)
    assert err_unknown.error_code == S3Errors.INTERNAL_ERROR
    assert err_unknown.message == "Tea pot."

    # 3. Numeric code mapping (e.g. 404 for NoSuchBucket)
    exc_404 = make_client_error("404", 404, "Not Found")
    err_404 = S3ErrorResponse.from_client_error(exc_404)
    assert err_404.error_code == S3Errors.NO_SUCH_BUCKET
    assert err_404.message == "Not Found"


def test_from_client_error_edge_cases() -> None:
    """Verify from_client_error handles abnormal exception inputs without response dicts."""
    # 1. Exception with no response attribute at all
    exc_generic = Exception("Generic unhandled exception")
    err = S3ErrorResponse.from_client_error(exc_generic)
    assert err.error_code == S3Errors.INTERNAL_ERROR
    assert err.message == "Generic unhandled exception"

    # 2. ClientError with non-dict response attribute
    exc_bad_response = ClientError({}, "PutObject")
    exc_bad_response.response = "bad"  # type: ignore[assignment]
    err_bad = S3ErrorResponse.from_client_error(exc_bad_response)
    assert err_bad.error_code == S3Errors.INTERNAL_ERROR
