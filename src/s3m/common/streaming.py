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


class AWSChunkedDecoder:
    """
    Decodes STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked) streams on the fly.
    """

    def __init__(self, reader: ASGIStreamReader) -> None:
        self.reader = reader
        self._buffer = bytearray()
        self._eof = False
        self._current_chunk_remaining = 0
        self._state = "READ_HEADER"  # READ_HEADER, READ_DATA, READ_CRLF

    async def _fill_buffer(self, n: int) -> bool:
        """Attempt to fill buffer with at least n bytes. Returns True if achieved, False if EOF."""
        while len(self._buffer) < n:
            chunk = await self.reader.read(65536)
            if not chunk:
                return False
            self._buffer.extend(chunk)
        return True

    async def read(self, n: int = -1) -> bytes:  # noqa: PLR0912
        if self._eof:
            return b""

        result = bytearray()

        while True:
            if n != -1 and len(result) >= n:
                break

            if self._state == "READ_HEADER":
                # Find \r\n
                newline_idx = self._buffer.find(b"\r\n")
                while newline_idx == -1:
                    chunk = await self.reader.read(65536)
                    if not chunk:
                        msg = "Unexpected EOF while reading aws-chunked header"
                        raise ValueError(msg)
                    self._buffer.extend(chunk)
                    newline_idx = self._buffer.find(b"\r\n")

                header_line = self._buffer[:newline_idx]
                del self._buffer[: newline_idx + 2]

                # Header looks like: 10000;chunk-signature=...
                size_str = header_line.split(b";")[0].strip()
                try:
                    self._current_chunk_remaining = int(size_str, 16)
                except ValueError as err:
                    msg = f"Invalid chunk size: {size_str}"
                    raise ValueError(msg) from err

                if self._current_chunk_remaining == 0:
                    self._eof = True
                    break
                self._state = "READ_DATA"

            elif self._state == "READ_DATA":
                to_read = self._current_chunk_remaining
                if n != -1:
                    to_read = min(to_read, n - len(result))

                await self._fill_buffer(to_read)
                actual_read = min(to_read, len(self._buffer))
                if actual_read == 0:
                    msg = "Unexpected EOF while reading aws-chunked data"
                    raise ValueError(msg)

                result.extend(self._buffer[:actual_read])
                del self._buffer[:actual_read]
                self._current_chunk_remaining -= actual_read

                if self._current_chunk_remaining == 0:
                    self._state = "READ_CRLF"

            elif self._state == "READ_CRLF":
                crlf_len = 2
                await self._fill_buffer(crlf_len)
                if len(self._buffer) < crlf_len or self._buffer[:crlf_len] != b"\r\n":
                    msg = "Missing CRLF after chunk data"
                    raise ValueError(msg)
                del self._buffer[:crlf_len]
                self._state = "READ_HEADER"

        return bytes(result)
