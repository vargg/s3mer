"""Declarative error classification rules for S3MER."""

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from http import HTTPStatus


class ErrorAction(StrEnum):
    """Action to take upon encountering an error."""

    RETRY = "retry"
    FALLBACK = "fallback"
    FAIL = "fail"


HTTP_CLIENT_ERROR_MIN = 400
HTTP_SERVER_ERROR_MIN = 500


@dataclass(frozen=True, slots=True)
class ErrorRule:
    """Single rule mapping exception traits to an ErrorAction."""

    action: ErrorAction
    codes: tuple[str, ...] = ()
    status_codes: tuple[int, ...] = ()
    status_range: tuple[int, int] | None = None
    exception_types: tuple[type[BaseException], ...] = ()
    predicate: Callable[[Exception], bool] | None = None

    def matches(self, exc: Exception) -> bool:
        if self.predicate is not None and self.predicate(exc):
            return True

        if self.exception_types and isinstance(exc, self.exception_types):
            return True

        response = getattr(exc, "response", None)
        if not isinstance(response, dict):
            return False

        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        error_code = response.get("Error", {}).get("Code", "")

        if self.codes and error_code in self.codes:
            return True
        if self.status_codes and status_code in self.status_codes:
            return True
        if self.status_range is not None:
            low, high = self.status_range
            if low <= status_code < high:
                return True
        return False


def _client_error_4xx(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return HTTP_CLIENT_ERROR_MIN <= status_code < HTTP_SERVER_ERROR_MIN


def _client_error_5xx(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return status_code >= HTTP_SERVER_ERROR_MIN


def _client_error_missing_response(exc: Exception) -> bool:
    from botocore.exceptions import ClientError  # noqa: PLC0415

    if not isinstance(exc, ClientError):
        return False
    response = getattr(exc, "response", None)
    return not isinstance(response, dict)


ERROR_REGISTRY: list[ErrorRule] = [
    ErrorRule(
        action=ErrorAction.RETRY,
        codes=("RequestLimitExceeded", "SlowDown", "RequestTimeout", "Throttling"),
        status_codes=(HTTPStatus.TOO_MANY_REQUESTS, HTTPStatus.SERVICE_UNAVAILABLE),
    ),
    ErrorRule(action=ErrorAction.FAIL, predicate=_client_error_missing_response),
    ErrorRule(action=ErrorAction.FAIL, status_range=(HTTP_CLIENT_ERROR_MIN, HTTP_SERVER_ERROR_MIN)),
    ErrorRule(action=ErrorAction.FALLBACK, predicate=_client_error_5xx),
    ErrorRule(action=ErrorAction.FAIL, predicate=_client_error_4xx),
]


def classify_from_registry(exc: Exception, registry: list[ErrorRule] | None = None) -> ErrorAction | None:
    """Return the first matching action from the registry, or None if no rule matched."""
    rules = registry if registry is not None else ERROR_REGISTRY
    for rule in rules:
        if rule.matches(exc):
            return rule.action
    return None
