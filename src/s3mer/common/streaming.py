"""Async streaming utilities for proxying S3 object bodies."""

import asyncio
import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from dataclasses import dataclass
from enum import Enum, auto
from functools import lru_cache
from pathlib import Path
from typing import Any, Self

from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker
from s3mer.common.types import Receive
from s3mer.config.settings import Settings, load_settings

logger = get_logger(__name__)

# Default chunk size: 64 KB — good balance between throughput and memory
DEFAULT_CHUNK_SIZE = 65_536
DEFAULT_MAX_MEMORY_STREAM_BUFFER_SIZE = 10 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Streaming buffer and chunk settings resolved from application config."""

    chunk_size: int
    max_memory_size: int
    buffer_dir: str | None

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            chunk_size=settings.stream_chunk_size,
            max_memory_size=settings.max_memory_stream_buffer_size,
            buffer_dir=settings.buffer_dir,
        )

    @classmethod
    def defaults(cls) -> Self:
        return cls(
            chunk_size=DEFAULT_CHUNK_SIZE,
            max_memory_size=DEFAULT_MAX_MEMORY_STREAM_BUFFER_SIZE,
            buffer_dir=None,
        )


@lru_cache
def get_stream_config() -> StreamConfig:
    """Return cached streaming settings, falling back to defaults if config is unavailable."""
    try:
        return StreamConfig.from_settings(load_settings())
    except Exception:
        return StreamConfig.defaults()


def reset_stream_config_cache() -> None:
    """Clear cached stream config (e.g. after settings override changes in tests)."""
    get_stream_config.cache_clear()


def get_chunk_size() -> int:
    """Get the streaming chunk size from configuration."""
    return get_stream_config().chunk_size


def get_max_memory_size() -> int:
    """Get the max memory buffer size before spooling from configuration."""
    return get_stream_config().max_memory_size


def get_buffer_dir() -> str | None:
    """Get the temporary file buffer directory from configuration."""
    return get_stream_config().buffer_dir


async def stream_s3_body(
    s3_response: dict[str, Any],
    chunk_size: int | None = None,
) -> AsyncGenerator[bytes, None]:
    """
    Yield chunks from an aiobotocore StreamingBody response.

    aiobotocore wraps aiohttp's StreamReader. We use iter_chunks()
    for efficient chunked reading without buffering the full object.
    """
    if chunk_size is None:
        chunk_size = get_stream_config().chunk_size
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
        metrics: MetricsTracker,
        *,
        stream_config: StreamConfig | None = None,
        max_memory_size: int | None = None,
        buffer_dir: str | None = None,
        chunk_size: int | None = None,
    ) -> None:
        config = stream_config or get_stream_config()
        resolved_max_memory = max_memory_size if max_memory_size is not None else config.max_memory_size
        resolved_buffer_dir = buffer_dir if buffer_dir is not None else config.buffer_dir
        self.reader = reader
        self._metrics = metrics
        self._tmp_file = tempfile.SpooledTemporaryFile(  # noqa: SIM115
            max_size=resolved_max_memory,
            mode="w+b",
            dir=resolved_buffer_dir,
        )
        self._read_from_tmp = False
        self._eof_reached = False
        self._chunk_size = chunk_size if chunk_size is not None else config.chunk_size

        self._metrics.record_active_stream_readers(1)

    def seek_to_start(self) -> None:
        """Switch to reading from the buffer and reset position to start."""
        if self._read_from_tmp:
            logger.debug("Rewinding already buffered stream")
        else:
            logger.debug("Switching to replay mode for buffered stream")
            self._read_from_tmp = True
        self._tmp_file.seek(0)

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Required by botocore to reset the stream during retries/fallbacks.
        We support seeking within the already-buffered data.
        """
        if offset == 0 and whence == 0:
            self.seek_to_start()
            return 0

        # Delegate to the underlying spool file for other seek types.
        # Note: Seeking forward past the buffer is not supported until EOF.
        return self._tmp_file.seek(offset, whence)

    async def read(self, n: int = -1) -> bytes:
        """Read n bytes from the current source (stream or buffer)."""
        if self._read_from_tmp:
            # Try to read from the buffer first
            chunk = self._tmp_file.read(n)
            if chunk:
                return chunk

            if not self._eof_reached:
                self._read_from_tmp = False
                logger.debug("Buffer exhausted, resuming live read from source")
            else:
                return b""

        if self._eof_reached:
            return b""

        try:
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
        chunk = await self.read(self._chunk_size)
        if not chunk:
            raise StopAsyncIteration
        return chunk

    def seekable(self) -> bool:
        """Explicitly signal that this stream supports seeking (required by some IO wrappers)."""
        return True

    def close(self) -> None:
        """Close and delete the temporary buffer."""
        self._tmp_file.close()
        self._metrics.record_active_stream_readers(-1)

    def tell(self) -> int:
        """Required for some botocore validations, though we are technically unseekable."""
        return self._tmp_file.tell()


class ASGIStreamReader(AsyncIterator[bytes]):
    """An async stream reader that reads from the ASGI receive channel."""

    def __init__(
        self,
        receive: Receive,
        on_read: Callable[[int], None] | None = None,
        chunk_size: int | None = None,
    ) -> None:
        self.receive = receive
        self._buffer = bytearray()
        self._more_body = True
        self._on_read = on_read
        self._chunk_size = chunk_size if chunk_size is not None else get_stream_config().chunk_size

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
            result = b"".join(chunks)
            self._buffer.clear()
            if self._on_read:
                self._on_read(len(result))
            return result

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
        if self._on_read:
            self._on_read(len(chunk))
        return chunk

    def __aiter__(self) -> Self:
        """Make it iterable for async iteration."""
        return self

    async def __anext__(self) -> bytes:
        """Async iterator protocol."""
        chunk = await self.read(self._chunk_size)
        if not chunk:
            raise StopAsyncIteration
        return chunk


class DecoderState(Enum):
    """Internal states for the aws-chunked decoder."""

    READ_HEADER = auto()
    READ_DATA = auto()
    READ_CRLF = auto()


class AWSChunkedDecoder(AsyncIterator[bytes]):
    """
    Decodes STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked) streams on the fly.
    """

    def __init__(self, reader: ASGIStreamReader, chunk_size: int | None = None) -> None:
        self.reader = reader
        self._raw_buffer = bytearray()
        self._decoded_buffer = bytearray()
        self._eof = False
        self._current_chunk_remaining = 0
        self._state = DecoderState.READ_HEADER
        self._chunk_size = chunk_size if chunk_size is not None else get_stream_config().chunk_size

    async def _fill_raw_buffer(self, n: int) -> bool:
        """Attempt to fill raw buffer with at least n bytes. Returns True if achieved, False if EOF."""
        while len(self._raw_buffer) < n:
            chunk = await self.reader.read(self._chunk_size)
            if not chunk:
                return False
            self._raw_buffer.extend(chunk)
        return True

    async def _handle_read_header(self) -> bool:
        """Handle the READ_HEADER state. Returns False on EOF/error."""
        if not await self._fill_raw_buffer(1):
            return False
        pos = self._raw_buffer.find(b"\r\n")
        if pos == -1:
            return await self._fill_raw_buffer(len(self._raw_buffer) + 128)

        header = self._raw_buffer[:pos]
        del self._raw_buffer[: pos + 2]
        try:
            size_hex = header.split(b";")[0]
            self._current_chunk_remaining = int(size_hex, 16)
        except (ValueError, IndexError):
            return False

        if self._current_chunk_remaining == 0:
            return False

        self._state = DecoderState.READ_DATA
        return True

    async def _handle_read_data(self) -> bool:
        """Handle the READ_DATA state. Returns False on EOF."""
        if not await self._fill_raw_buffer(1):
            return False
        take = min(len(self._raw_buffer), self._current_chunk_remaining)
        self._decoded_buffer.extend(self._raw_buffer[:take])
        del self._raw_buffer[:take]
        self._current_chunk_remaining -= take
        if self._current_chunk_remaining == 0:
            self._state = DecoderState.READ_CRLF
        return True

    async def _handle_read_crlf(self) -> bool:
        """Handle the READ_CRLF state (chunk delimiter). Returns False on EOF."""
        if not await self._fill_raw_buffer(2):
            return False
        del self._raw_buffer[:2]
        self._state = DecoderState.READ_HEADER
        return True

    async def read(self, n: int = -1) -> bytes:
        if n == -1:
            chunks = []
            while not self._eof or self._decoded_buffer:
                chunk = await self.read(self._chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)

        while len(self._decoded_buffer) < n and not self._eof:
            match self._state:
                case DecoderState.READ_HEADER:
                    if not await self._handle_read_header():
                        self._eof = True
                case DecoderState.READ_DATA:
                    if not await self._handle_read_data():
                        self._eof = True
                case DecoderState.READ_CRLF:
                    if not await self._handle_read_crlf():
                        self._eof = True

        result = bytes(self._decoded_buffer[:n])
        del self._decoded_buffer[:n]
        return result

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read(self._chunk_size)
        if not chunk:
            raise StopAsyncIteration
        return chunk


class AsyncBytesReader(AsyncIterator[bytes]):
    """Replayable async reader over shared in-memory bytes (one allocation, many iterators)."""

    def __init__(self, data: bytes, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        self._data = data
        self._offset = 0
        self._chunk_size = chunk_size

    async def read(self, n: int = -1) -> bytes:
        if self._offset >= len(self._data):
            return b""
        if n == -1:
            chunk = self._data[self._offset :]
            self._offset = len(self._data)
            return chunk
        end = min(self._offset + n, len(self._data))
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read(self._chunk_size)
        if not chunk:
            raise StopAsyncIteration
        return chunk


def _write_spill_file(path: str, prefix_chunks: list[bytes], middle_chunk: bytes, suffix_chunks: list[bytes]) -> None:
    with Path(path).open("wb") as spill_file:
        spill_file.writelines(prefix_chunks)
        spill_file.write(middle_chunk)
        spill_file.writelines(suffix_chunks)


class MultiSyncBodyBuffer:
    """
    Buffer a request body once for fan-out to multiple backends.

    Small bodies stay in memory (shared bytes); larger bodies spill to a temp file.
    """

    def __init__(
        self,
        *,
        data: bytes | None = None,
        path: str | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self._data = data
        self._path = path
        self._chunk_size = chunk_size
        self._readers: list[AsyncIterator[bytes]] = []

    @classmethod
    async def from_body(cls, body: Any, stream_config: StreamConfig) -> Self | None:
        """Materialize an async stream or bytes body for multi-backend replay."""
        if body is None:
            return None
        if isinstance(body, (bytes, bytearray)):
            return cls(data=bytes(body), chunk_size=stream_config.chunk_size)
        if not isinstance(body, AsyncIterator):
            return None

        chunks: list[bytes] = []
        total = 0
        max_memory = stream_config.max_memory_size

        async for chunk in body:
            total += len(chunk)
            if total <= max_memory:
                chunks.append(chunk)
                continue

            temp_fd, temp_path = tempfile.mkstemp(prefix="s3mer_multisync_", dir=stream_config.buffer_dir)
            os.close(temp_fd)

            rest_chunks = [rest async for rest in body]
            await asyncio.to_thread(_write_spill_file, temp_path, chunks, chunk, rest_chunks)
            return cls(path=temp_path, chunk_size=stream_config.chunk_size)

        return cls(data=b"".join(chunks), chunk_size=stream_config.chunk_size)

    def open_reader(self) -> AsyncIterator[bytes]:
        """Return a new independent reader positioned at the start."""
        if self._data is not None:
            reader: AsyncIterator[bytes] = AsyncBytesReader(self._data, self._chunk_size)
        elif self._path is not None:
            reader = ConcurrentFileStream(self._path, chunk_size=self._chunk_size)
        else:
            raise RuntimeError("MultiSyncBodyBuffer has no data")
        self._readers.append(reader)
        return reader

    async def close(self) -> None:
        """Close readers and remove any spilled temp file."""
        for reader in self._readers:
            if isinstance(reader, ConcurrentFileStream):
                with contextlib.suppress(Exception):
                    await reader.close()
        self._readers.clear()

        if self._path is not None:
            temp_path = Path(self._path)
            if await asyncio.to_thread(temp_path.exists):
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(temp_path.unlink)


class ConcurrentFileStream(AsyncIterator[bytes]):
    """
    An async iterator that reads from a file on disk.
    Each instance has its own file handle, allowing concurrent reads.
    """

    def __init__(self, filepath: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        self.filepath = filepath
        self.chunk_size = chunk_size
        self._file: Any = None

    def _ensure_file(self) -> None:
        if self._file is None:
            self._file = Path(self.filepath).open("rb")  # noqa: SIM115

    async def read(self, n: int = -1) -> bytes:
        await asyncio.to_thread(self._ensure_file)
        return await asyncio.to_thread(self._file.read, n)

    def seek(self, offset: int, whence: int = 0) -> int:
        self._ensure_file()
        return self._file.seek(offset, whence)

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        self._ensure_file()
        return self._file.tell()

    async def __anext__(self) -> bytes:
        chunk = await self.read(self.chunk_size)
        if not chunk:
            await self.close()
            raise StopAsyncIteration
        return chunk

    async def close(self) -> None:
        if self._file is not None:
            await asyncio.to_thread(self._file.close)
            self._file = None
