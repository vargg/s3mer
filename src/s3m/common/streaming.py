"""Async streaming utilities for proxying S3 object bodies."""

from collections.abc import AsyncGenerator
from typing import Any

# Default chunk size: 64 KB — good balance between throughput and memory
DEFAULT_CHUNK_SIZE = 65_536


async def stream_s3_body(
    s3_response: dict,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncGenerator[bytes, None]:
    """
    Yield chunks from an aiobotocore StreamingBody response.

    aiobotocore wraps aiohttp's StreamReader. We use iter_chunks()
    for efficient chunked reading without buffering the full object.
    """
    stream = s3_response["Body"]
    try:
        while True:
            chunk = await stream.content.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        stream.close()


async def collect_request_body(
    body_iterator: AsyncGenerator[bytes, None],
) -> bytes:
    """
    Collect an async body stream into bytes.

    Use only for small payloads (bucket creation XML, etc.).
    For large objects, stream directly to the backend.
    """
    chunks: list[bytes] = [chunk async for chunk in body_iterator]
    return b"".join(chunks)


class ASGIStreamReader:
    """An async stream reader that reads from the ASGI receive channel."""

    def __init__(self, receive: Any) -> None:
        self.receive = receive
        self._buffer = bytearray()
        self._more_body = True

    async def read(self, n: int = -1) -> bytes:
        if n == -1:
            chunks = [self._buffer]
            while self._more_body:
                msg = await self.receive()
                if msg["type"] == "http.request":
                    chunks.append(msg.get("body", b""))
                    self._more_body = msg.get("more_body", False)
                elif msg["type"] == "http.disconnect":
                    self._more_body = False
                    break
            self._buffer.clear()
            return b"".join(chunks)

        while len(self._buffer) < n and self._more_body:
            msg = await self.receive()
            if msg["type"] == "http.request":
                self._buffer.extend(msg.get("body", b""))
                self._more_body = msg.get("more_body", False)
            elif msg["type"] == "http.disconnect":
                self._more_body = False
                break

        chunk = bytes(self._buffer[:n])
        del self._buffer[:n]
        return chunk
