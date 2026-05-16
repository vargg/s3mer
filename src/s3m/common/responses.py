from collections.abc import AsyncGenerator, Callable

from s3m.common.types import Receive, Scope, Send


class ASGIResponse:
    """A plain ASGI HTTP response."""

    def __init__(
        self,
        content: bytes = b"",
        status_code: int = 200,
        media_type: str = "application/xml",
        headers: dict[str, str] | None = None,
        on_bytes_sent: Callable[[int], None] | None = None,
    ) -> None:
        self.body = content
        self.status_code = status_code
        self.media_type = media_type
        self.extra_headers = headers or {}
        self.on_bytes_sent = on_bytes_sent

    async def __call__(self, _scope: Scope, _receive: Receive, send: Send) -> None:
        extra_lower = {k.lower(): v for k, v in self.extra_headers.items()}
        headers: list[tuple[bytes, bytes]] = []

        if "content-type" not in extra_lower:
            headers.append((b"content-type", self.media_type.encode()))

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

        if self.on_bytes_sent:
            self.on_bytes_sent(len(self.body))


class ASGIStreamingResponse:
    """An ASGI HTTP response that streams the body via an async generator."""

    def __init__(
        self,
        generator: AsyncGenerator[bytes, None],
        status_code: int = 200,
        media_type: str = "application/octet-stream",
        headers: dict[str, str] | None = None,
        on_bytes_sent: Callable[[int], None] | None = None,
    ) -> None:
        self._generator = generator
        self.status_code = status_code
        self.media_type = media_type
        self.extra_headers = headers or {}
        self.on_bytes_sent = on_bytes_sent

    async def __call__(self, _scope: Scope, _receive: Receive, send: Send) -> None:
        extra_lower = {k.lower(): v for k, v in self.extra_headers.items()}
        resp_headers: list[tuple[bytes, bytes]] = []

        if "content-type" not in extra_lower:
            resp_headers.append((b"content-type", self.media_type.encode()))

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
            if self.on_bytes_sent:
                self.on_bytes_sent(len(chunk))

        await send(
            {
                "type": "http.response.body",
                "body": b"",
                "more_body": False,
            },
        )
