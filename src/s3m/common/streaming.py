"""Async streaming utilities for proxying S3 object bodies."""

import tempfile
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any, Self

from s3m.common.logging import get_logger

logger = get_logger(__name__)

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


class BufferedStreamReader(AsyncIterator[bytes]):
    """
    Wraps an async stream and buffers all read data to a temporary file.
    Allows the stream to be 'replayed' once by calling seek_to_start().
    """

    def __init__(
        self,
        reader: AsyncIterator[bytes],
        max_memory_size: int = 10 * 1024 * 1024,  # 10 MB
    ) -> None:
        self.reader = reader
        self._tmp_file = tempfile.SpooledTemporaryFile(max_size=max_memory_size, mode="w+b")  # noqa: SIM115
        self._read_from_tmp = False
        self._eof_reached = False

    def seek_to_start(self) -> None:
        """Switch to reading from the buffer and reset position to start."""
        if self._read_from_tmp:
            logger.debug("Rewinding already buffered stream")
        else:
            logger.debug("Switching to replay mode for buffered stream")
            self._read_from_tmp = True
        self._tmp_file.seek(0)

    async def read(self, n: int = -1) -> bytes:
        """Read n bytes from the current source (stream or buffer)."""
        if self._read_from_tmp:
            # SpooledTemporaryFile.read is sync, but aiobotocore handles it
            return self._tmp_file.read(n)

        if self._eof_reached:
            return b""

        # Read from source stream
        try:
            # We use a fixed chunk size when reading from the underlying reader
            # regardless of 'n', to simplify buffering.
            chunk = await anext(self.reader)
        except (StopAsyncIteration, GeneratorExit):
            self._eof_reached = True
            return b""
        else:
            self._tmp_file.write(chunk)
            return chunk

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read(DEFAULT_CHUNK_SIZE)
        if not chunk:
            raise StopAsyncIteration
        return chunk

    def close(self) -> None:
        """Close and delete the temporary buffer."""
        self._tmp_file.close()

    def tell(self) -> int:
        """Required for some botocore validations, though we are technically unseekable."""
        return self._tmp_file.tell()


class ASGIStreamReader(AsyncIterator[bytes]):
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

    def __aiter__(self) -> Self:
        """Make it iterable for async iteration."""
        return self

    async def __anext__(self) -> bytes:
        """Async iterator protocol."""
        chunk = await self.read(DEFAULT_CHUNK_SIZE)
        if not chunk:
            raise StopAsyncIteration
        return chunk


class AWSChunkedDecoder(AsyncIterator[bytes]):
    """
    Decodes STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked) streams on the fly.
    """

    def __init__(self, reader: ASGIStreamReader) -> None:
        self.reader = reader
        self._raw_buffer = bytearray()
        self._decoded_buffer = bytearray()
        self._eof = False
        self._current_chunk_remaining = 0
        self._state = "READ_HEADER"  # READ_HEADER, READ_DATA, READ_CRLF

    async def _fill_raw_buffer(self, n: int) -> bool:
        """Attempt to fill raw buffer with at least n bytes. Returns True if achieved, False if EOF."""
        while len(self._raw_buffer) < n:
            chunk = await self.reader.read(DEFAULT_CHUNK_SIZE)
            if not chunk:
                return False
            self._raw_buffer.extend(chunk)
        return True

    async def read(self, n: int = -1) -> bytes:  # noqa: PLR0912
        if n == -1:
            chunks = []
            while not self._eof or self._decoded_buffer:
                chunk = await self.read(DEFAULT_CHUNK_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)

        while len(self._decoded_buffer) < n and not self._eof:
            if self._state == "READ_HEADER":
                if not await self._fill_raw_buffer(1):
                    self._eof = True
                    break
                pos = self._raw_buffer.find(b"\r\n")
                if pos == -1:
                    if not await self._fill_raw_buffer(len(self._raw_buffer) + 128):
                        self._eof = True
                        break
                    continue
                header = self._raw_buffer[:pos]
                del self._raw_buffer[: pos + 2]
                try:
                    size_hex = header.split(b";")[0]
                    self._current_chunk_remaining = int(size_hex, 16)
                except (ValueError, IndexError):
                    self._eof = True
                    break
                if self._current_chunk_remaining == 0:
                    self._eof = True
                    break
                self._state = "READ_DATA"

            elif self._state == "READ_DATA":
                if not await self._fill_raw_buffer(1):
                    self._eof = True
                    break
                take = min(len(self._raw_buffer), self._current_chunk_remaining)
                self._decoded_buffer.extend(self._raw_buffer[:take])
                del self._raw_buffer[:take]
                self._current_chunk_remaining -= take
                if self._current_chunk_remaining == 0:
                    self._state = "READ_CRLF"

            elif self._state == "READ_CRLF":
                if not await self._fill_raw_buffer(2):
                    self._eof = True
                    break
                del self._raw_buffer[:2]
                self._state = "READ_HEADER"

        result = bytes(self._decoded_buffer[:n])
        del self._decoded_buffer[:n]
        return result

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read(DEFAULT_CHUNK_SIZE)
        if not chunk:
            raise StopAsyncIteration
        return chunk
