"""Simple ASGI response types for the S3 proxy.

These are standalone — no framework dependency.
"""

from collections.abc import AsyncGenerator
from typing import Any


class ASGIResponse:
    """A plain ASGI HTTP response."""

    def __init__(
        self,
        content: bytes = b"",
        status_code: int = 200,
        media_type: str = "application/xml",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.body = content
        self.status_code = status_code
        self.media_type = media_type
        self.extra_headers = headers or {}

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:  # noqa: ARG002
        # Check if Content-Length is provided explicitly in headers
        extra_lower = {k.lower(): v for k, v in self.extra_headers.items()}
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", self.media_type.encode()),
        ]
        if "content-length" not in extra_lower:
            headers.append((b"content-length", str(len(self.body)).encode()))

        for k, v in self.extra_headers.items():
            headers.append((k.lower().encode(), v.encode()))

        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": headers,
            },
        )
        await send(
            {
                "type": "http.response.body",
                "body": self.body,
            },
        )


class ASGIStreamingResponse:
    """An ASGI HTTP response that streams the body via an async generator."""

    def __init__(
        self,
        generator: AsyncGenerator[bytes, None],
        status_code: int = 200,
        media_type: str = "application/octet-stream",
        headers: dict[str, str] | None = None,
    ) -> None:
        self._generator = generator
        self.status_code = status_code
        self.media_type = media_type
        self.extra_headers = headers or {}

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:  # noqa: ARG002
        resp_headers: list[tuple[bytes, bytes]] = [
            (b"content-type", self.media_type.encode()),
        ]
        for k, v in self.extra_headers.items():
            resp_headers.append((k.lower().encode(), v.encode()))

        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": resp_headers,
            },
        )

        async for chunk in self._generator:
            await send(
                {
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": True,
                },
            )

        await send(
            {
                "type": "http.response.body",
                "body": b"",
                "more_body": False,
            },
        )
