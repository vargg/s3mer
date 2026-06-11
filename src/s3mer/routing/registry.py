"""Registry for S3 handlers and their metadata."""

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from s3mer.backends.pool import BackendPool
from s3mer.backends.strategies import (
    OperationStrategy,
    ReadFallbackStrategy,
)
from s3mer.common.metrics import MetricsTracker
from s3mer.common.responses import ASGIResponse, ASGIStreamingResponse
from s3mer.routing.operations import S3Operation


class BodyStyle(Enum):
    """How the request body should be handled before calling the handler."""

    STREAM = auto()  # ASGIStreamReader / AWSChunkedDecoder
    BUFFERED = auto()  # Read entire body into bytes
    EMPTY = auto()  # No body expected/read


@dataclass(frozen=True, slots=True)
class HandlerContext:
    """
    Standard context passed to all S3 handlers.

    This replaces individual arguments and avoids the need for
    reflection/DI while keeping the dispatcher simple.
    """

    operation: S3Operation
    bucket: str
    key: str | None
    pool: BackendPool
    read_strategy: ReadFallbackStrategy
    write_strategy: OperationStrategy
    metrics: MetricsTracker
    headers: dict[str, str]
    query_string: bytes
    body: Any = None  # bytes or ASGIStreamReader
    content_length: int | None = None


type HandlerFunc = Callable[[HandlerContext], Coroutine[Any, Any, ASGIResponse | ASGIStreamingResponse]]


@dataclass(frozen=True, slots=True)
class HandlerMetadata:
    """Metadata about a registered handler."""

    operation: S3Operation
    func: HandlerFunc
    body_style: BodyStyle
    is_object_op: bool


class HandlerRegistry:
    """Central registry for S3 handlers."""

    def __init__(self) -> None:
        self._handlers: dict[S3Operation, HandlerMetadata] = {}

    def register(
        self,
        operation: S3Operation,
        body_style: BodyStyle = BodyStyle.EMPTY,
        is_object_op: bool = True,
    ) -> Callable[[HandlerFunc], HandlerFunc]:
        """Decorator to register a handler."""

        def decorator(func: HandlerFunc) -> HandlerFunc:
            self._handlers[operation] = HandlerMetadata(
                operation=operation,
                func=func,
                body_style=body_style,
                is_object_op=is_object_op,
            )
            return func

        return decorator

    def get(self, operation: S3Operation) -> HandlerMetadata | None:
        """Get handler metadata for an operation."""
        return self._handlers.get(operation)

    @property
    def handlers(self) -> dict[S3Operation, HandlerMetadata]:
        """All registered handlers."""
        return self._handlers


registry = HandlerRegistry()
s3_handler = registry.register
